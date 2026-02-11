#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""run_bq_param.py wrapper with minimal spinner keepalive.

- Does NOT modify run_bq_param.py.
- While `bq query` runs, prints only a single-character spinner `|/-`.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import run_bq_param as base


def run_bq_query_with_params(
    sql_path: Path,
    program_id: str,
    standard_date: str,
    target_table: str,
    job_seq: str,
    temp_table: str,
) -> None:
    sql_text = sql_path.read_text(encoding="utf-8", errors="replace")

    cmd = [
        "bq",
        "query",
        "--quiet",
        "--use_legacy_sql=false",
        "--parameter=program_id:STRING:{}".format(program_id),
        "--parameter=standard_date:STRING:{}".format(standard_date),
        "--parameter=target_table:STRING:{}".format(target_table),
        "--parameter=job_seq:STRING:{}".format(job_seq),
        "--parameter=temp_table:STRING:{}".format(temp_table),
    ]

    spinner = "|/-"
    spin_i = 0

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        universal_newlines=True,
    )

    try:
        assert proc.stdin is not None
        proc.stdin.write(sql_text)
        proc.stdin.close()

        while True:
            try:
                proc.wait(timeout=2)
                break
            except subprocess.TimeoutExpired:
                sys.stderr.write("\r" + spinner[spin_i % len(spinner)])
                sys.stderr.flush()
                spin_i += 1
    except KeyboardInterrupt:
        proc.terminate()
        raise
    finally:
        sys.stderr.write("\r \r")
        sys.stderr.flush()

    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd)


def main() -> int:
    base.run_bq_query_with_params = run_bq_query_with_params  # type: ignore[attr-defined]
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())
