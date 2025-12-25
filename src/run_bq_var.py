#!/usr/bin/env python3
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple, Optional


BASE_DIR = Path("/home/bskim/hc")


class ListItem(NamedTuple):
    sql_rel: str
    job_dt: str
    tbl_id: str


def quote_bq_string(value: str) -> str:
    # BigQuery string literal escaping: single quote doubled
    return "'" + value.replace("'", "''") + "'"


def parse_list_line(line: str) -> Optional[ListItem]:
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    parts = re.split(r"\s+", line)
    if len(parts) < 3:
        raise ValueError(f"Invalid list line (need: <sql> <job_dt> <tbl_id>): {line}")

    sql_rel, job_dt, tbl_id = parts[0], parts[1], parts[2]
    return ListItem(sql_rel=sql_rel, job_dt=job_dt, tbl_id=tbl_id)


def load_sql(sql_path: Path) -> str:
    return sql_path.read_text(encoding="utf-8")


def substitute_sql(template: str, *, pgm_id: str, job_dt: str, tbl_id: str) -> str:
    return (
        template.replace("{vs_pgm_id}", quote_bq_string(pgm_id))
        .replace("{vs_job_dt}", quote_bq_string(job_dt))
        .replace("{vs_tbl_id}", quote_bq_string(tbl_id))
    )


def run_bq(sql_text: str) -> None:
    subprocess.run(
        ["bq", "query", "--quiet", "--use_legacy_sql=false"],
        input=sql_text,
        universal_newlines=True,
        check=True,
    )


def main() -> int:
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <sql.list>")
        return 1

    list_file = Path(sys.argv[1])
    if not list_file.exists():
        print(f"[FAIL] List file not found: {list_file}")
        return 1

    total = 0
    success = 0
    fail = 0

    for raw_line in list_file.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            item = parse_list_line(raw_line)
            if item is None:
                continue

            sql_path = BASE_DIR / "sql" / item.sql_rel
            if not sql_path.exists():
                print(f"[FAIL] File not found: {sql_path}")
                fail += 1
                continue

            pgm_id = Path(item.sql_rel).stem
            sql_text = substitute_sql(load_sql(sql_path), pgm_id=pgm_id, job_dt=item.job_dt, tbl_id=item.tbl_id)

            total += 1
            print(f"[INFO] {item.sql_rel} (job_dt={item.job_dt}, tbl_id={item.tbl_id})")
            run_bq(sql_text)
            print()
            success += 1

        except ValueError as e:
            print(f"[FAIL] {e}")
            fail += 1
        except subprocess.CalledProcessError as e:
            print(f"[FAIL] bq query failed (exit_code={e.returncode})")
            fail += 1

    print(f"[SUMMARY] total={total}, success={success}, fail={fail}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
