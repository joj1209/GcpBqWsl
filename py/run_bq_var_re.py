#!/usr/bin/env python3
import re
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple, Optional


# ============================
# Config & Constants
# ============================
class Config:
    BASE_DIR = Path("/home/bskim/hc")
    SQL_DIR = BASE_DIR / "sql"


# ============================
# Data Record (Python 3.6 compatible, immutable)
# ============================
class ListItem(NamedTuple):
    sql_rel: str
    job_dt: str
    tbl_id: str


# ============================
# Utility Functions
# ============================
def quote_bq_string(value: str) -> str:
    """Escape and quote a string for BigQuery SQL"""
    return "'" + value.replace("'", "''") + "'"


def parse_list_line(line: str) -> Optional[ListItem]:
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    parts = re.split(r"\s+", line)
    if len(parts) < 3:
        raise ValueError(f"Invalid line (expected: <sql> <job_dt> <tbl_id>): {line}")

    return ListItem(sql_rel=parts[0], job_dt=parts[1], tbl_id=parts[2])


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


def log_info(message: str) -> None:
    print(f"[INFO] {message}")


def log_fail(message: str) -> None:
    print(f"[FAIL] {message}")


# ============================
# Main Runner Class
# ============================
class BqJobRunner:
    def __init__(self, list_file: Path):
        self.list_file = list_file
        self.total = 0
        self.success = 0
        self.fail = 0

    def run(self) -> int:
        if not self.list_file.exists():
            log_fail(f"List file not found: {self.list_file}")
            return 1

        for raw_line in self.list_file.read_text(encoding="utf-8", errors="replace").splitlines():
            self.process_line(raw_line)

        self.print_summary()
        return 0 if self.fail == 0 else 1

    def process_line(self, raw_line: str) -> None:
        try:
            item = parse_list_line(raw_line)
            if item is None:
                return

            sql_path = Config.SQL_DIR / item.sql_rel
            if not sql_path.exists():
                log_fail(f"SQL file not found: {sql_path}")
                self.fail += 1
                return

            pgm_id = Path(item.sql_rel).stem
            template = sql_path.read_text(encoding="utf-8")
            sql_text = substitute_sql(
                template,
                pgm_id=pgm_id,
                job_dt=item.job_dt,
                tbl_id=item.tbl_id,
            )

            log_info(f"{item.sql_rel} (job_dt={item.job_dt}, tbl_id={item.tbl_id})")
            run_bq_query(sql_text)
            print()

            self.success += 1
            self.total += 1

        except ValueError as e:
            log_fail(str(e))
            self.fail += 1

        except subprocess.CalledProcessError as e:
            log_fail(f"bq query failed (exit_code={e.returncode})")
            self.fail += 1

    def print_summary(self) -> None:
        print(f"[SUMMARY] total={self.total}, success={self.success}, fail={self.fail}")


# ============================
# Entry Point
# ============================
def main() -> int:
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <sql.list>")
        return 1

    list_file = Path(sys.argv[1])
    runner = BqJobRunner(list_file)
    return runner.run()


if __name__ == "__main__":
    raise SystemExit(main())
