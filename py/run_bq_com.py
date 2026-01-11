#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path


logger = logging.getLogger(__name__)


class Config(object):
    BASE_DIR = Path(__file__).resolve().parents[1]
    ENV_DIR = BASE_DIR / "src" / "env"

    MID_ENV_JSON = ENV_DIR / "mid_env.json"
    MID_JSON_FALLBACK = ENV_DIR / "mid.json"

    DW_LIST = ENV_DIR / "dw" / "bq_sql.list"
    DM_LIST = ENV_DIR / "dm" / "bq_sql.list"

    DW_JSON = ENV_DIR / "dw" / "bq_sql.json"
    DM_JSON = ENV_DIR / "dm" / "bq_sql.json"

    SQL_DIR = BASE_DIR / "sql"


class LazyErrorFileHandler(logging.Handler):
    """Create .log.err only if an error occurs."""

    def __init__(self, err_path, formatter):
        logging.Handler.__init__(self, level=logging.ERROR)
        self._err_path = Path(err_path)
        self._formatter = formatter
        self._fh = None

    def emit(self, record):
        try:
            if self._fh is None:
                self._err_path.parent.mkdir(parents=True, exist_ok=True)
                self._fh = logging.FileHandler(str(self._err_path), encoding="utf-8")
                self._fh.setLevel(logging.ERROR)
                self._fh.setFormatter(self._formatter)
            self._fh.emit(record)
        except Exception:
            self.handleError(record)

    def close(self):
        try:
            if self._fh is not None:
                self._fh.close()
        finally:
            logging.Handler.close(self)


def setup_logging(base_dir, mid_for_name):
    run_date = datetime.now().strftime("%Y%m%d")
    log_dir = Path(base_dir) / "log" / run_date
    log_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%H%M%S")
    base = f"run_bq_com_{mid_for_name}_{stamp}"

    out_log = log_dir / (base + ".log")
    err_log = log_dir / (base + ".log.err")

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    for h in list(root.handlers):
        root.removeHandler(h)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)

    file_out = logging.FileHandler(str(out_log), encoding="utf-8")
    file_out.setLevel(logging.INFO)
    file_out.setFormatter(fmt)

    lazy_err = LazyErrorFileHandler(err_log, fmt)

    root.addHandler(console)
    root.addHandler(file_out)
    root.addHandler(lazy_err)

    return out_log, err_log


def parse_cli_args(argv):
    args = {}
    for token in argv:
        if "=" not in token:
            raise ValueError(f"Invalid arg (expected key=value): {token}")
        key, value = token.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise ValueError(f"Invalid arg (empty key): {token}")
        args[key] = value
    return args


def read_list_csv(list_path):
    list_path = Path(list_path)
    if not list_path.exists():
        raise FileNotFoundError(f"List file not found: {list_path}")

    text = list_path.read_text(encoding="utf-8", errors="replace")
    text = text.lstrip("\ufeff")

    reader = csv.DictReader(text.splitlines())
    records = []

    for row in reader:
        if not row or not any(row.values()):
            continue
        first_val = next(iter(row.values()), "")
        if str(first_val).strip().startswith("#"):
            continue

        cleaned = {}
        for k, v in row.items():
            if not k:
                continue
            cleaned[k.strip()] = (v or "").strip()
        records.append(cleaned)

    return records


def save_json(json_path, records):
    json_path = Path(json_path)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(records, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def load_mid_env():
    path = Config.MID_ENV_JSON if Config.MID_ENV_JSON.exists() else Config.MID_JSON_FALLBACK
    if not path.exists():
        raise FileNotFoundError(f"Missing mid env json: {path}")

    data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    if not isinstance(data, dict):
        raise ValueError("Invalid mid env json format")

    return path, data


def normalize_use_yn(v):
    return (v or "Y").strip().upper() == "Y"


def record_matches_pgm_id(record, pgm_id):
    if not pgm_id:
        return True

    target = pgm_id.strip()

    rec_mid = (record.get("mid") or "").strip()
    rec_pgm = (record.get("pgm_id") or "").strip()

    candidates = [rec_pgm]
    if rec_mid and rec_pgm:
        candidates.append(rec_mid + "/" + rec_pgm)

    return target in candidates


def split_mid_and_name(pgm_id):
    s = (pgm_id or "").strip()
    if "/" in s:
        left, right = s.split("/", 1)
        return left.strip(), right.strip()
    return "", s


def resolve_sql_path(program_type, record_mid, pgm_id):
    sql_root = Config.SQL_DIR / program_type

    mid_from_id, name = split_mid_and_name(pgm_id)
    if mid_from_id:
        return sql_root / mid_from_id / name

    return sql_root / record_mid / name


def build_standard_date(cli_args, mid_env_section, record):
    # CLI override (standard key: job_d)
    if "job_d" in cli_args and cli_args["job_d"]:
        return cli_args["job_d"].strip()

    # mid_env override if set
    env_date = (mid_env_section.get("job_d") or "").strip()
    if env_date:
        return env_date

    # record baseline
    return (record.get("job_d") or "").strip()


def build_params(program_type, mid_env_section, record, cli_args):
    params = {}

    # common
    pgm_id = (record.get("pgm_id") or "").strip()
    _, filename = split_mid_and_name(pgm_id)
    program_id = Path(filename).stem

    standard_date = build_standard_date(cli_args, mid_env_section, record)

    if program_type == "dw":
        # allow both param-name and list-name overrides
        target_table = (
            cli_args.get("target_table")
            or record.get("target_table")
            or ""
        ).strip()
        job_seq = (
            cli_args.get("job_seq")
            or record.get("job_seq")
            or ""
        ).strip()
        temp_table = (
            cli_args.get("temp_table")
            or record.get("temp_table")
            or ""
        ).strip()

        params.update(
            {
                "program_id": program_id,
                "standard_date": standard_date,
                "target_table": target_table,
                "job_seq": job_seq,
                "temp_table": temp_table,
            }
        )
    elif program_type == "dm":
        table_name = (
            cli_args.get("table_name")
            or cli_args.get("tbl_id")
            or record.get("tbl_id")
            or ""
        ).strip()
        params.update(
            {
                "program_id": program_id,
                "standard_date": standard_date,
                "table_name": table_name,
            }
        )
    else:
        raise ValueError(f"Unknown program_type: {program_type}")

    # only include keys declared in mid_env params (dynamic allocation)
    declared = mid_env_section.get("params") or []
    declared = [str(x) for x in declared]

    return {k: params.get(k, "") for k in declared}


def run_bq_query(sql_path, params_dict):
    sql_text = Path(sql_path).read_text(encoding="utf-8", errors="replace")

    cmd = [
        "bq",
        "query",
        "--quiet",
        "--use_legacy_sql=false",
    ]

    for k, v in params_dict.items():
        cmd.append(f"--parameter={k}:STRING:{v}")

    subprocess.run(cmd, input=sql_text, universal_newlines=True, check=True)


def generate_baselines():
    dw_records = read_list_csv(Config.DW_LIST)
    dm_records = read_list_csv(Config.DM_LIST)

    save_json(Config.DW_JSON, dw_records)
    save_json(Config.DM_JSON, dm_records)

    return dw_records, dm_records


def filter_targets(records, cli_args):
    # use_yn=Y only
    targets = [r for r in records if normalize_use_yn(r.get("use_yn"))]

    # mid filter
    if "mid" in cli_args and cli_args["mid"] and cli_args["mid"] != "all":
        mid = cli_args["mid"].strip()
        targets = [r for r in targets if (r.get("mid") or "").strip() == mid]

    # pgm_id filter
    if "pgm_id" in cli_args and cli_args["pgm_id"]:
        pgm_id = cli_args["pgm_id"].strip()
        targets = [r for r in targets if record_matches_pgm_id(r, pgm_id)]

    return targets


def execute_jobs(program_type, mid_env_section, targets, cli_args):
    total = 0
    success = 0
    fail = 0
    done = 0

    for record in targets:
        record_mid = (record.get("mid") or "").strip()
        pgm_id = (record.get("pgm_id") or "").strip()

        total += 1

        sql_path = resolve_sql_path(program_type, record_mid, pgm_id)
        if not sql_path.exists():
            logger.error("SQL file not found: %s", sql_path)
            fail += 1
            done += 1
            continue

        params_dict = build_params(program_type, mid_env_section, record, cli_args)

        logger.info("RUN %s %s (mid=%s)", program_type, pgm_id, record_mid)
        logger.info("PARAMS %s", params_dict)

        try:
            run_bq_query(sql_path, params_dict)
            success += 1
        except subprocess.CalledProcessError as e:
            logger.error("bq query failed (exit_code=%s)", e.returncode)
            fail += 1
        except Exception as e:
            logger.error("job failed: %s", e)
            fail += 1
        finally:
            done += 1

    return total, done, success, fail


def main():
    # Parse CLI
    try:
        cli_args = parse_cli_args(sys.argv[1:]) if len(sys.argv) > 1 else {}
    except ValueError as e:
        print(str(e))
        print(
            f"Usage: python {sys.argv[0]} mid=<mid|dw_all|dm_all> [pgm_id=<mid/file.sql|file.sql>] [job_d=<yyyymmdd>] ..."
        )
        return 1

    if not cli_args:
        print("At least one key=value argument is required (mid or pgm_id).")
        return 1

    if "mid" not in cli_args and "pgm_id" not in cli_args:
        print("mid is required unless pgm_id is provided.")
        return 1

    if (cli_args.get("mid") or "").strip() == "all":
        print("mid=all is no longer supported. Use mid=dw_all or mid=dm_all.")
        return 1

    mid_for_log = (cli_args.get("mid") or "nomid").strip() or "nomid"
    out_log, err_log = setup_logging(Config.BASE_DIR, mid_for_log)

    logger.info("SUCCESS LOG : %s", out_log)
    logger.info("ERROR LOG   : %s (created only on error)", err_log)
    logger.info("CLI ARGS    : %s", cli_args)

    # Load env
    try:
        env_path, mid_env = load_mid_env()
        logger.info("MID ENV     : %s", env_path)
    except Exception as e:
        logger.error("%s", e)
        return 1

    # Generate json baselines
    try:
        dw_records, dm_records = generate_baselines()
        logger.info("Generated baseline: %s", Config.DW_JSON)
        logger.info("Generated baseline: %s", Config.DM_JSON)
    except Exception as e:
        logger.error("%s", e)
        return 1

    # Determine what to run
    want_mid = (cli_args.get("mid") or "").strip()
    run_dw_all = want_mid == "dw_all"
    run_dm_all = want_mid == "dm_all"

    totals = {"total": 0, "done": 0, "success": 0, "fail": 0}

    # DW
    dw_section = mid_env.get("dw") or {}
    dw_cli_args = dict(cli_args)
    if run_dw_all:
        dw_cli_args.pop("mid", None)
    dw_targets = filter_targets(dw_records, dw_cli_args)

    if run_dw_all or not want_mid or (want_mid and want_mid in [m for m in (dw_section.get("mids") or [])]):
        if not run_dm_all and dw_targets:
            t, d, s, f = execute_jobs("dw", dw_section, dw_targets, dw_cli_args)
            totals["total"] += t
            totals["done"] += d
            totals["success"] += s
            totals["fail"] += f

    # DM
    dm_section = mid_env.get("dm") or {}
    dm_cli_args = dict(cli_args)
    if run_dm_all:
        dm_cli_args.pop("mid", None)
    dm_targets = filter_targets(dm_records, dm_cli_args)

    if run_dm_all or not want_mid or (want_mid and want_mid in [m for m in (dm_section.get("mids") or [])]):
        if not run_dw_all and dm_targets:
            t, d, s, f = execute_jobs("dm", dm_section, dm_targets, dm_cli_args)
            totals["total"] += t
            totals["done"] += d
            totals["success"] += s
            totals["fail"] += f

    if totals["total"] == 0:
        logger.warning("No targets matched filters: %s", cli_args)
        return 0

    logger.info("=" * 60)
    logger.info(
        "PROGRAM_TOTAL=%d, EXEC_DONE=%d, SUCCESS=%d, FAIL=%d",
        totals["total"],
        totals["done"],
        totals["success"],
        totals["fail"],
    )

    return 1 if totals["fail"] > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
