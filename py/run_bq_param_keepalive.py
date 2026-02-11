#!/usr/bin/env python3
"""run_bq_param_keepalive.py

Purpose
- Same behavior as py/run_bq_param.py, but prints periodic heartbeat logs while a
  long-running `bq query` is executing, so the console does not appear idle.

Env vars
- BQ_HEARTBEAT_SEC: seconds between heartbeat logs (default: 2)
- BQ_QUIET: if truthy, add `--quiet` to bq query (default: 1 to preserve current behavior)

Examples
- Default (quiet + heartbeat every 2s):
  python3 py/run_bq_param_keepalive.py mid=... vs_job_dt=20260211

- Let bq print its own status updates too:
  BQ_QUIET=0 python3 py/run_bq_param_keepalive.py ...

- Heartbeat every 10s:
  BQ_HEARTBEAT_SEC=10 python3 py/run_bq_param_keepalive.py ...
"""

import csv
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


class Config:
    BASE_DIR = Path(__file__).resolve().parents[1]
    SQL_DIR = BASE_DIR / "sql_param"
    CSV_PATH = BASE_DIR / "src" / "list" / "bq.csv"
    JSON_PATH = BASE_DIR / "src" / "list" / "bq.json"


class _MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int):
        super().__init__()
        self._max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self._max_level


def setup_logging(base_dir: Path) -> Tuple[Path, Path]:
    run_date = datetime.now().strftime("%Y%m%d")
    log_dir = base_dir / "log" / run_date
    log_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%H%M%S")
    base = "run_bq_param_keepalive.{}.{}".format(stamp, os.getpid())

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

    file_out = logging.FileHandler(out_log, encoding="utf-8")
    file_out.setLevel(logging.INFO)
    file_out.setFormatter(fmt)
    file_out.addFilter(_MaxLevelFilter(logging.WARNING))

    file_err = logging.FileHandler(err_log, encoding="utf-8")
    file_err.setLevel(logging.ERROR)
    file_err.setFormatter(fmt)

    root.addHandler(console)
    root.addHandler(file_out)
    root.addHandler(file_err)

    return out_log, err_log


def read_csv_records(csv_path: Path) -> List[Dict[str, str]]:
    if not csv_path.exists():
        raise FileNotFoundError("CSV file not found: {}".format(csv_path))

    text = csv_path.read_text(encoding="utf-8", errors="replace")
    text = text.lstrip("\ufeff")

    reader = csv.DictReader(text.splitlines())
    records: List[Dict[str, str]] = []

    for row in reader:
        if not row or not any(row.values()):
            continue
        first_val = next(iter(row.values()), "")
        if first_val.strip().startswith("#"):
            continue
        records.append({k: v.strip() for k, v in row.items() if k})

    return records


def save_json(json_path: Path, records: List[Dict[str, str]]) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(records, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def parse_cli_args(argv: List[str]) -> Dict[str, str]:
    args: Dict[str, str] = {}
    for token in argv:
        if "=" not in token:
            raise ValueError("Invalid arg (expected key=value): {}".format(token))
        key, value = token.split("=", 1)
        args[key.strip()] = value.strip()
    return args


def _env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "y", "yes", "on")


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None or not val.strip():
        return default
    try:
        return int(val)
    except ValueError:
        return default


def run_bq_query_with_params(
    sql_path: Path,
    program_id: str,
    standard_date: str,
    target_table: str,
    job_seq: str,
    temp_table: str,
) -> None:
    sql_text = sql_path.read_text(encoding="utf-8", errors="replace")

    bq_quiet = _env_bool("BQ_QUIET", True)
    heartbeat_sec = max(1, _env_int("BQ_HEARTBEAT_SEC", 2))

    cmd: List[str] = [
        "bq",
        "query",
        "--use_legacy_sql=false",
        "--parameter=program_id:STRING:{}".format(program_id),
        "--parameter=standard_date:STRING:{}".format(standard_date),
        "--parameter=target_table:STRING:{}".format(target_table),
        "--parameter=job_seq:STRING:{}".format(job_seq),
        "--parameter=temp_table:STRING:{}".format(temp_table),
    ]
    if bq_quiet:
        cmd.insert(2, "--quiet")

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        text=True,
    )
    assert proc.stdin is not None
    proc.stdin.write(sql_text)
    proc.stdin.close()

    start = time.monotonic()
    next_beat = start + heartbeat_sec

    try:
        while proc.poll() is None:
            now = time.monotonic()
            if now >= next_beat:
                elapsed = int(now - start)
                hh = elapsed // 3600
                mm = (elapsed % 3600) // 60
                ss = elapsed % 60
                logger.info("bq query running... elapsed=%02d:%02d:%02d", hh, mm, ss)
                next_beat = now + heartbeat_sec
            time.sleep(1)
    except KeyboardInterrupt:
        logger.error("Interrupted; terminating bq process")
        proc.terminate()
        raise

    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd)


def apply_filters(records: List[Dict[str, str]], cli_args: Dict[str, str]) -> List[Dict[str, str]]:
    filtered = [r for r in records if r.get("use_yn", "Y").upper() == "Y"]

    if "mid" in cli_args:
        target_mid = cli_args["mid"]
        filtered = [r for r in filtered if r.get("mid", "") == target_mid]

    if "vs_pgm_id" in cli_args:
        target_pgm = cli_args["vs_pgm_id"]
        filtered = [r for r in filtered if r.get("vs_pgm_id", "") == target_pgm]

    return filtered


def execute_sql_jobs(targets: List[Dict[str, str]], overrides: Dict[str, str]) -> Tuple[int, int, int]:
    total = success = fail = 0

    for record in targets:
        effective = dict(record)
        effective.update(overrides)

        vs_pgm_id = effective.get("vs_pgm_id", "").strip()
        vs_job_dt = effective.get("vs_job_dt", "").strip()
        vs_tbl_id = effective.get("vs_tbl_id", "").strip()
        job_seq = effective.get("job_seq", "1").strip()
        temp_table = effective.get("temp_table", "").strip()

        if not vs_pgm_id:
            logger.error("Missing vs_pgm_id in record: %s", effective)
            fail += 1
            continue

        sql_path = Config.SQL_DIR / vs_pgm_id
        if not sql_path.exists():
            logger.error("SQL file not found: %s", sql_path)
            fail += 1
            continue

        total += 1
        logger.info(
            "%s (mid=%s, vs_job_dt=%s, vs_tbl_id=%s, job_seq=%s, temp_table=%s)",
            vs_pgm_id,
            effective.get("mid", ""),
            vs_job_dt,
            vs_tbl_id,
            job_seq,
            temp_table,
        )

        try:
            program_id = sql_path.stem
            run_bq_query_with_params(
                sql_path=sql_path,
                program_id=program_id,
                standard_date=vs_job_dt,
                target_table=vs_tbl_id,
                job_seq=job_seq,
                temp_table=temp_table,
            )
            success += 1
        except subprocess.CalledProcessError as e:
            logger.error("bq query failed (exit_code=%s)", e.returncode)
            fail += 1

    return total, success, fail


def main() -> int:
    out_log, err_log = setup_logging(Config.BASE_DIR)
    logger.info("SUCCESS LOG : %s", out_log)
    logger.info("ERROR LOG   : %s", err_log)

    try:
        cli_args = parse_cli_args(sys.argv[1:]) if len(sys.argv) > 1 else {}
    except ValueError as e:
        logger.error("%s", e)
        logger.error(
            "Usage: python %s [mid=<mid>] [vs_pgm_id=<file.sql>] [vs_job_dt=<yyyymmdd>] [job_seq=<seq>] [temp_table=<table>]",
            sys.argv[0],
        )
        return 1

    try:
        records = read_csv_records(Config.CSV_PATH)
        save_json(Config.JSON_PATH, records)
        logger.info("Generated JSON baseline: %s", Config.JSON_PATH)
    except Exception as e:
        logger.error("%s", e)
        return 1

    targets = apply_filters(records, cli_args)
    if not targets:
        logger.warning("No targets matched filters: %s", cli_args)
        return 0

    logger.info("Targets matched: %d", len(targets))

    overrides = {k: v for k, v in cli_args.items() if k not in ("mid", "vs_pgm_id")}

    total, success, fail = execute_sql_jobs(targets, overrides)

    logger.info("=" * 50)
    logger.info("Total: %d, Success: %d, Fail: %d", total, success, fail)

    return 1 if fail > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
