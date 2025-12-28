#!/usr/bin/env python3
import csv
import json
import logging
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple


logger = logging.getLogger(__name__)


# ============================
# Config & Paths
# ============================
class Config:
    # Script lives in: <BASE_DIR>/py/run_bq_var_json.py
    BASE_DIR = Path(__file__).resolve().parents[1]
    SQL_DIR = BASE_DIR / "sql"
    CSV_PATH = BASE_DIR / "src" / "list" / "bq.csv"
    JSON_PATH = BASE_DIR / "src" / "list" / "bq.json"


# ============================
# Logging
# ============================
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
    base = "run_bq_var_json.{}.{}".format(stamp, os.getpid())

    out_log = log_dir / (base + ".log")
    err_log = log_dir / (base + ".log.err")

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Prevent duplicate handlers if main() is called multiple times.
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


# ============================
# CSV <-> JSON
# ============================
def _strip_bom(text: str) -> str:
    return text.lstrip("\ufeff")


def read_csv_records(csv_path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    if not csv_path.exists():
        raise FileNotFoundError("CSV file not found: {}".format(csv_path))

    lines = csv_path.read_text(encoding="utf-8", errors="replace").splitlines()
    if not lines:
        return ([], [])

    lines[0] = _strip_bom(lines[0])

    reader = csv.reader(lines)
    rows = list(reader)
    if not rows:
        return ([], [])

    headers = [h.strip() for h in rows[0] if h.strip()]
    if not headers:
        raise ValueError("CSV header is empty: {}".format(csv_path))

    records: List[Dict[str, str]] = []

    for row in rows[1:]:
        if not row:
            continue

        # treat full-line comments (starts with '#')
        if row[0].strip().startswith("#"):
            continue

        cells = [c.strip() for c in row]
        if all(not c for c in cells):
            continue

        values: List[str]

        if len(cells) == len(headers):
            values = cells

        elif len(cells) < len(headers):
            # Fallback for "CSV" lines that only use one comma and then spaces.
            # Example (headers=4):
            #   qa,bq_dw_red_care_sales_01.sql 20251223 DW.RED_CARE_SALES
            head = cells[:-1]
            tail = cells[-1]
            tail_parts = [p for p in re.split(r"\s+", tail) if p]
            values = head + tail_parts

            if len(values) != len(headers):
                raise ValueError(
                    "Invalid CSV row: expected {} columns but got {} after normalization: {}".format(
                        len(headers), len(values), row
                    )
                )

        else:
            # Too many columns: keep early fields, join the rest into the last field.
            values = cells[: len(headers) - 1] + [",".join(cells[len(headers) - 1 :]).strip()]

        rec = {headers[i]: (values[i].strip() if i < len(values) else "") for i in range(len(headers))}
        records.append(rec)

    return headers, records


def write_json_records(json_path: Path, records: List[Dict[str, str]]) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(records, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def load_json_records(json_path: Path) -> List[Dict[str, str]]:
    if not json_path.exists():
        raise FileNotFoundError("JSON file not found: {}".format(json_path))
    return json.loads(json_path.read_text(encoding="utf-8"))


# ============================
# CLI parsing
# ============================
def parse_kv_args(argv: List[str]) -> Dict[str, str]:
    args: Dict[str, str] = {}
    for token in argv:
        if "=" not in token:
            raise ValueError("Invalid arg (expected key=value): {}".format(token))
        key, value = token.split("=", 1)
        args[key.strip()] = value.strip()
    return args


def usage() -> str:
    name = Path(sys.argv[0]).name
    return (
        "Usage:\n"
        "  python {} [mid=<mid>] [vs_pgm_id=<sql_file>] [vs_job_dt=<yyyymmdd>]\n\n".format(name)
        + "Examples:\n"
        + "  python {} mid=qa\n".format(name)
        + "  python {} vs_pgm_id=bq_dw_red_care_sales_01.sql\n".format(name)
        + "  python {} vs_pgm_id=bq_dw_red_care_sales_01.sql vs_job_dt=20251201\n".format(name)
        + "  python {} vs_job_dt=20251202\n".format(name)
    )


# ============================
# BigQuery execution helpers
# ============================
def quote_bq_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def substitute_sql(template: str, *, pgm_id: str, job_dt: str, tbl_id: str) -> str:
    return (
        template.replace("{vs_pgm_id}", quote_bq_string(pgm_id))
        .replace("{vs_job_dt}", quote_bq_string(job_dt))
        .replace("{vs_tbl_id}", quote_bq_string(tbl_id))
    )


def run_bq_query(sql_text: str) -> None:
    subprocess.run(
        ["bq", "query", "--quiet", "--use_legacy_sql=false"],
        input=sql_text,
        universal_newlines=True,
        check=True,
    )


# ============================
# Core logic
# ============================
def is_use_enabled(record: Dict[str, str]) -> bool:
    # Requirement: common filter is use_yn=Y
    # If CSV/JSON doesn't have use_yn column, treat it as enabled.
    use_yn = record.get("use_yn")
    if use_yn is None:
        return True
    return use_yn.strip().upper() == "Y"


def resolve_sql_path(vs_pgm_id: str) -> Path:
    # Typical: bq_dw_red_care_sales_01.sql
    candidate = Path(vs_pgm_id)

    if candidate.is_absolute():
        return candidate

    # If user accidentally passes bare name without .sql, try appending.
    if candidate.suffix.lower() != ".sql":
        candidate = Path(vs_pgm_id + ".sql")

    # Prefer SQL_DIR
    return Config.SQL_DIR / candidate


def main() -> int:
    out_log, err_log = setup_logging(Config.BASE_DIR)
    logger.info("SUCCESS LOG : %s", out_log)
    logger.info("ERROR LOG   : %s", err_log)

    try:
        cli = parse_kv_args(sys.argv[1:]) if len(sys.argv) > 1 else {}
    except ValueError as e:
        logger.error("%s", e)
        logger.error("%s", usage())
        return 1

    # 1) Create JSON baseline from CSV (source of truth)
    try:
        _, records_from_csv = read_csv_records(Config.CSV_PATH)
        write_json_records(Config.JSON_PATH, records_from_csv)
        logger.info("Generated JSON baseline: %s", Config.JSON_PATH)
    except Exception as e:
        logger.error("%s", e)
        return 1

    # 2) Load JSON baseline
    try:
        records = load_json_records(Config.JSON_PATH)
    except Exception as e:
        logger.error("%s", e)
        return 1

    # 3) Filter: use_yn=Y
    targets = [r for r in records if is_use_enabled(r)]

    # 4) Filters from CLI
    mid_filter = cli.get("mid")
    if mid_filter:
        targets = [r for r in targets if r.get("mid") == mid_filter]

    pgm_filter = cli.get("vs_pgm_id")
    if pgm_filter:
        targets = [r for r in targets if r.get("vs_pgm_id") == pgm_filter]

    if not targets:
        logger.error("No target rows matched the given filters (and use_yn=Y).")
        return 1

    # 5) Overrides (if not provided, JSON value is used)
    overrides = {k: v for k, v in cli.items() if k not in ("mid", "vs_pgm_id")}

    total = 0
    success = 0
    fail = 0

    for base in targets:
        effective = dict(base)
        effective.update(overrides)

        vs_pgm_id = (effective.get("vs_pgm_id") or "").strip()
        vs_job_dt = (effective.get("vs_job_dt") or "").strip()
        vs_tbl_id = (effective.get("vs_tbl_id") or "").strip()

        if not vs_pgm_id:
            logger.error("Missing vs_pgm_id in record: %s", effective)
            fail += 1
            continue

        sql_path = resolve_sql_path(vs_pgm_id)
        if not sql_path.exists():
            logger.error("SQL file not found: %s", sql_path)
            fail += 1
            continue

        pgm_id_value = sql_path.stem
        template = sql_path.read_text(encoding="utf-8", errors="replace")
        sql_text = substitute_sql(template, pgm_id=pgm_id_value, job_dt=vs_job_dt, tbl_id=vs_tbl_id)

        total += 1
        logger.info(
            "%s (mid=%s, vs_job_dt=%s, vs_tbl_id=%s)",
            vs_pgm_id,
            effective.get("mid", ""),
            vs_job_dt,
            vs_tbl_id,
        )

        try:
            run_bq_query(sql_text)
            success += 1
        except subprocess.CalledProcessError as e:
            logger.error("bq query failed (exit_code=%s)", e.returncode)
            fail += 1

    logger.info("SUMMARY total=%s, success=%s, fail=%s", total, success, fail)
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
