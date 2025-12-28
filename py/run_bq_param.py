#!/usr/bin/env python3
import logging
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, Optional


logger = logging.getLogger(__name__)


# ============================
# Config
# ============================
class Config:
    BASE_DIR = Path(__file__).resolve().parents[1]
    SQL_DIR = BASE_DIR / "sql"


# ============================
# Logging
# ============================
def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


# ============================
# Core Logic
# ============================
def parse_list_line(line: str) -> Optional[tuple]:
    """Parse a line from .list file into (sql_file, job_dt, tbl_id)."""
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    parts = re.split(r"\s+", line)
    if len(parts) < 3:
        raise ValueError("Invalid line (expected: <sql> <job_dt> <tbl_id>): {}".format(line))

    return (parts[0], parts[1], parts[2])


def run_bq_query(sql_text: str, params: Dict[str, str]) -> None:
    """Execute BigQuery with named parameters."""
    # Convert {placeholder} -> @param
    sql_param = sql_text
    for key in params:
        sql_param = sql_param.replace("{{{}}}".format(key), "@{}".format(key))

    # Build --parameter flags for parameters actually used in SQL
    flags = []
    for name, value in params.items():
        if "@{}".format(name) in sql_param:
            flags.append("--parameter={}:STRING:{}".format(name, value))

    subprocess.run(
        ["bq", "query", "--quiet", "--use_legacy_sql=false"] + flags,
        input=sql_param,
        universal_newlines=True,
        check=True,
    )


def process_sql_file(sql_path: Path, job_dt: str, tbl_id: str) -> None:
    """Read SQL file and execute with parameters."""
    if not sql_path.exists():
        raise FileNotFoundError("SQL file not found: {}".format(sql_path))

    sql_text = sql_path.read_text(encoding="utf-8")
    pgm_id = sql_path.stem

    run_bq_query(sql_text, {
        "vs_pgm_id": pgm_id,
        "vs_job_dt": job_dt,
        "vs_tbl_id": tbl_id,
    })


# ============================
# Runners
# ============================
def run_list_mode(list_file: Path) -> int:
    """Execute multiple SQL files from a .list file."""
    if not list_file.exists():
        logger.error("List file not found: %s", list_file)
        return 1

    total = success = fail = 0

    for line in list_file.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            parsed = parse_list_line(line)
            if parsed is None:
                continue

            sql_file, job_dt, tbl_id = parsed
            sql_path = Config.SQL_DIR / sql_file

            logger.info("%s (job_dt=%s, tbl_id=%s)", sql_file, job_dt, tbl_id)
            process_sql_file(sql_path, job_dt, tbl_id)

            success += 1
            total += 1

        except (ValueError, FileNotFoundError) as e:
            logger.error("%s", e)
            fail += 1
        except subprocess.CalledProcessError as e:
            logger.error("bq query failed (exit_code=%s)", e.returncode)
            fail += 1

    logger.info("SUMMARY total=%s, success=%s, fail=%s", total, success, fail)
    return 0 if fail == 0 else 1


def run_sql_mode(sql_arg: str, job_dt: Optional[str] = None, tbl_id: Optional[str] = None) -> int:
    """Execute a single SQL file."""
    # Resolve path: absolute > cwd > SQL_DIR
    sql_path = Path(sql_arg)
    if not sql_path.is_absolute():
        if not sql_path.exists():
            sql_path = Config.SQL_DIR / sql_arg

    logger.info("%s", sql_path)

    try:
        process_sql_file(sql_path, job_dt or "", tbl_id or "")
    except FileNotFoundError as e:
        logger.error("%s", e)
        return 1
    except subprocess.CalledProcessError as e:
        logger.error("bq query failed (exit_code=%s)", e.returncode)
        return 1

    return 0


# ============================
# Entry Point
# ============================
def main() -> int:
    setup_logging()

    if len(sys.argv) < 2:
        logger.error("Usage: python %s <file.list | file.sql> [job_dt tbl_id]", sys.argv[0])
        return 1

    input_file = sys.argv[1]
    suffix = Path(input_file).suffix.lower()

    if suffix == ".list":
        if len(sys.argv) != 2:
            logger.error("Usage: python %s <file.list>", sys.argv[0])
            return 1
        return run_list_mode(Path(input_file))

    if suffix == ".sql":
        if len(sys.argv) == 2:
            return run_sql_mode(input_file)
        if len(sys.argv) == 4:
            return run_sql_mode(input_file, sys.argv[2], sys.argv[3])
        logger.error("Usage: python %s <file.sql> [job_dt tbl_id]", sys.argv[0])
        return 1

    logger.error("Unsupported file type: %s (expected .list or .sql)", input_file)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
