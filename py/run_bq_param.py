#!/usr/bin/env python3
import logging
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, NamedTuple, Optional


logger = logging.getLogger(__name__)


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
def parse_list_line(line: str) -> Optional[ListItem]:
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    parts = re.split(r"\s+", line)
    if len(parts) < 3:
        raise ValueError("Invalid line (expected: <sql> <job_dt> <tbl_id>): {}".format(line))

    return ListItem(sql_rel=parts[0], job_dt=parts[1], tbl_id=parts[2])


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def resolve_sql_path(arg: str) -> Path:
    """Resolve a SQL file path from a CLI argument.

    Resolution order:
    1) Absolute path as-is
    2) Relative path from current working directory
    3) Relative path under Config.SQL_DIR (keeps parity with .list mode)
    """
    candidate = Path(arg)
    if candidate.is_absolute():
        return candidate

    if candidate.exists():
        return candidate

    return Config.SQL_DIR / candidate


def render_parametrized_sql(template: str) -> str:
    """Convert template placeholders into BigQuery named parameters.

    Example:
      {vs_job_dt} -> @vs_job_dt
    """
    return (
        template.replace("{vs_pgm_id}", "@vs_pgm_id")
        .replace("{vs_job_dt}", "@vs_job_dt")
        .replace("{vs_tbl_id}", "@vs_tbl_id")
    )


def build_bq_parameter_flags(sql_text: str, values: Dict[str, str]) -> list:
    """Build --parameter flags for parameters that appear in the SQL text.

    BigQuery can error if you provide parameters that are not referenced.
    To avoid that, only include parameters that exist as @name in sql_text.
    """
    flags = []
    for name, value in values.items():
        needle = "@" + name
        if needle not in sql_text:
            continue
        flags.append("--parameter={}:STRING:{}".format(name, value))
    return flags


def warn_remaining_template_placeholders(sql_text: str) -> None:
    placeholders = ["{vs_pgm_id}", "{vs_job_dt}", "{vs_tbl_id}"]
    remaining = [p for p in placeholders if p in sql_text]
    if remaining:
        logger.warning(
            "SQL contains template placeholders %s. "
            "This script expects placeholders to be used as values and will convert them to BigQuery parameters.",
            remaining,
        )


def run_bq_query(sql_text: str, *, parameters: Dict[str, str]) -> None:
    sql_param = render_parametrized_sql(sql_text)

    warn_remaining_template_placeholders(sql_param)

    param_flags = build_bq_parameter_flags(sql_param, parameters)

    subprocess.run(
        ["bq", "query", "--quiet", "--use_legacy_sql=false"] + param_flags,
        input=sql_param,
        universal_newlines=True,
        check=True,
    )


# ============================
# Main Runner Class (.list mode)
# ============================
class BqParamRunner:
    def __init__(self, list_file: Path):
        self.list_file = list_file
        self.total = 0
        self.success = 0
        self.fail = 0

    def run(self) -> int:
        if not self.list_file.exists():
            logger.error("List file not found: %s", self.list_file)
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
                logger.error("SQL file not found: %s", sql_path)
                self.fail += 1
                return

            pgm_id = Path(item.sql_rel).stem
            template = sql_path.read_text(encoding="utf-8")

            logger.info("%s (job_dt=%s, tbl_id=%s)", item.sql_rel, item.job_dt, item.tbl_id)
            run_bq_query(
                template,
                parameters={
                    "vs_pgm_id": pgm_id,
                    "vs_job_dt": item.job_dt,
                    "vs_tbl_id": item.tbl_id,
                },
            )

            self.success += 1
            self.total += 1

        except ValueError as e:
            logger.error("%s", e)
            self.fail += 1

        except subprocess.CalledProcessError as e:
            logger.error("bq query failed (exit_code=%s)", e.returncode)
            self.fail += 1

    def print_summary(self) -> None:
        logger.info("SUMMARY total=%s, success=%s, fail=%s", self.total, self.success, self.fail)


# ============================
# Single-file runner (.sql mode)
# ============================
def run_single_sql(sql_file: Path, *, job_dt: Optional[str], tbl_id: Optional[str]) -> int:
    if not sql_file.exists():
        logger.error("SQL file not found: %s", sql_file)
        return 1

    sql_text = sql_file.read_text(encoding="utf-8")
    logger.info("%s", sql_file)

    pgm_id = sql_file.stem

    needs_job_dt = "{vs_job_dt}" in sql_text or "@vs_job_dt" in sql_text
    needs_tbl_id = "{vs_tbl_id}" in sql_text or "@vs_tbl_id" in sql_text

    if (needs_job_dt and not job_dt) or (needs_tbl_id and not tbl_id):
        logger.error(
            "This SQL appears to reference job_dt/tbl_id placeholders. "
            "Usage: python %s <sql.sql> <job_dt> <tbl_id>",
            sys.argv[0],
        )
        return 1

    try:
        run_bq_query(
            sql_text,
            parameters={
                "vs_pgm_id": pgm_id,
                "vs_job_dt": job_dt or "",
                "vs_tbl_id": tbl_id or "",
            },
        )
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
        logger.error("Usage: python %s <sql.list | sql.sql> [job_dt tbl_id]", sys.argv[0])
        return 1

    input_arg = sys.argv[1]
    input_path = Path(input_arg)
    suffix = input_path.suffix.lower()

    if suffix == ".list":
        if len(sys.argv) != 2:
            logger.error("Usage: python %s <sql.list>", sys.argv[0])
            return 1
        runner = BqParamRunner(input_path)
        return runner.run()

    if suffix == ".sql":
        if len(sys.argv) == 2:
            return run_single_sql(resolve_sql_path(input_arg), job_dt=None, tbl_id=None)
        if len(sys.argv) == 4:
            return run_single_sql(resolve_sql_path(input_arg), job_dt=sys.argv[2], tbl_id=sys.argv[3])

        logger.error("Usage: python %s <sql.sql> [job_dt tbl_id]", sys.argv[0])
        return 1

    logger.error("Unsupported input file type: %s (expected .list or .sql)", input_path)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
