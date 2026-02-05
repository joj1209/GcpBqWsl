#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Generate per-program verification SQL files from app/table.ini.

Requirements source: doc/요구사항_20260225.txt

- Reads app/table.ini (CSV header-based)
- Generates one SQL file per row into app/out/
- Output filename is "vrf_" + v_program_name
- Optional sections:
  - v_metric_sum_yn=Y: include SUM metrics for v_sum_col1/v_sum_col2 if provided
  - v_metric_cnt_yn=Y: include COUNT(DISTINCT) metrics for v_cnt_col1/v_cnt_col2 if provided
  - v_filter_yn=Y: include WHERE filters for v_filter_col1/v_filter_col2 if provided
- v_use_yn != Y rows are skipped

Python 3.6 compatible.
"""

import argparse
import csv
from pathlib import Path


def yn_is_true(value):
    return (value or "").strip().upper() == "Y"


def _strip(value):
    return (value or "").strip()


def normalize_identifier(value):
    """Return a BigQuery identifier quoted with backticks.

    - If empty -> ''
    - If it already contains '`' -> return as-is
    - Else -> wrap whole string in backticks

    Note: For table refs like BM.공통코드, quoting the whole string as `BM.공통코드`
    works in BigQuery.
    """

    s = _strip(value)
    if not s:
        return ""
    if "`" in s:
        return s
    return "`%s`" % s


def safe_filename(name):
    s = _strip(name)
    # prevent path traversal / accidental directories
    s = s.replace("/", "_").replace("\\", "_").replace(":", "_")
    return s


def read_table_ini(path):
    path = Path(path)
    text = path.read_text(encoding="utf-8", errors="replace")
    text = text.lstrip("\ufeff")

    reader = csv.DictReader(text.splitlines())
    records = []

    for row in reader:
        if not row:
            continue
        if not any((v or "").strip() for v in row.values()):
            continue

        cleaned = {}
        for k, v in row.items():
            if not k:
                continue
            cleaned[k.strip()] = (v or "").strip()
        records.append(cleaned)

    return records


def build_sql(record):
    mid = _strip(record.get("mid"))
    v_program_name = _strip(record.get("v_program_name"))
    v_stat_dt = _strip(record.get("v_stat_dt"))
    v_table_name_raw = _strip(record.get("v_table_name"))

    table_ref = normalize_identifier(v_table_name_raw)

    metric_sum = yn_is_true(record.get("v_metric_sum_yn"))
    metric_cnt = yn_is_true(record.get("v_metric_cnt_yn"))
    filter_yn = yn_is_true(record.get("v_filter_yn"))

    sum_cols = []
    if metric_sum:
        for key in ("v_sum_col1", "v_sum_col2"):
            col = normalize_identifier(record.get(key))
            if col:
                sum_cols.append(col)

    cnt_cols = []
    if metric_cnt:
        for key in ("v_cnt_col1", "v_cnt_col2"):
            col = normalize_identifier(record.get(key))
            if col:
                cnt_cols.append(col)

    filter_cols = []
    if filter_yn:
        for key in ("v_filter_col1", "v_filter_col2"):
            col = normalize_identifier(record.get(key))
            if col:
                filter_cols.append(col)

    metric_select_lines = []
    for col in cnt_cols:
        metric_select_lines.append("       , COUNT(DISTINCT %s) AS %s" % (col, col))
    for col in sum_cols:
        metric_select_lines.append("       , SUM(%s) AS %s" % (col, col))

    where_lines = []
    if filter_cols:
        for i, col in enumerate(filter_cols):
            prefix = "WHERE" if i == 0 else "  AND"
            where_lines.append(
                "   %s %s = PARSE_DATE('%%Y%%m%%d', '%s')" % (prefix, col, v_stat_dt)
            )

    metrics_struct_lines = []
    for col in cnt_cols + sum_cols:
        metrics_struct_lines.append("      %s AS %s" % (col, col))

    metrics_struct_sql = ""
    if metrics_struct_lines:
        metrics_struct_sql = ",\n    STRUCT(\n%s\n    ) AS METRICS" % ",\n".join(metrics_struct_lines)

    metric_select_sql = "\n".join(metric_select_lines)
    where_sql = "\n".join(where_lines)

    return (
        "-- AUTO-GENERATED\n"
        "-- source: app/table.ini\n"
        "-- mid: %s\n"
        "-- v_program_name: %s\n"
        "-- v_table_name: %s\n"
        "-- v_stat_dt: %s\n"
        "\n"
        "INSERT INTO U.T\n"
        "WITH SEQ_CTE AS (\n"
        "  SELECT COALESCE(MAX(SEQ), 0) + 1 AS NEXT_SEQ\n"
        "    FROM U.T\n"
        "   WHERE TBL_NM = \"%s\"\n"
        "     AND STAT_DT = PARSE_DATE('%%Y%%m%%d', '%s')\n"
        "),\n"
        "ALL_DATA AS (\n"
        "  SELECT COUNT(1) AS ALL_CNT\n"
        "    FROM %s\n"
        "),\n"
        "FILTER_DATA AS (\n"
        "  SELECT COUNT(1) AS FILTER_COUNT\n"
        "%s\n"
        "    FROM %s\n"
        "%s\n"
        "),\n"
        "JSON_CTE AS (\n"
        "  SELECT TO_JSON(STRUCT(\n"
        "    \"%s\" AS FILTER_TYPE,\n"
        "    \"%s\" AS FILTER_VALUE,\n"
        "    FILTER_COUNT AS FILTER_CNT"
        "%s\n"
        "  )) AS FILTER_JSON\n"
        "  FROM FILTER_DATA\n"
        ")\n"
        "SELECT \"%s\" AS PRG_NM\n"
        "     , \"%s\" AS TBL_NM\n"
        "     , PARSE_DATE('%%Y%%m%%d', '%s') AS STAT_DT\n"
        "     , SEQ_CTE.NEXT_SEQ AS SEQ\n"
        "     , ALL_DATA.ALL_CNT AS ALL_CNT\n"
        "     , JSON_CTE.FILTER_JSON AS STATS_CNT\n"
        "     , CURRENT_DATETIME('Asia/Seoul') AS INS_DTM\n"
        "  FROM SEQ_CTE, ALL_DATA, FILTER_DATA, JSON_CTE\n"
        ";\n"
        % (
            mid,
            v_program_name,
            v_table_name_raw,
            v_stat_dt,
            v_table_name_raw,
            v_stat_dt,
            table_ref,
            metric_select_sql,
            table_ref,
            ("\n" + where_sql) if where_sql else "",
            v_table_name_raw,
            v_stat_dt,
            metrics_struct_sql,
            v_program_name,
            v_table_name_raw,
            v_stat_dt,
        )
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description="Generate app/out/vrf_*.sql from app/table.ini")
    parser.add_argument("--table", default=str(Path("app") / "table.ini"), help="Input CSV (default: app/table.ini)")
    parser.add_argument("--out", default=str(Path("app") / "out"), help="Output directory (default: app/out)")
    args = parser.parse_args(argv)

    table_path = Path(args.table)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    records = read_table_ini(table_path)

    total = 0
    written = 0
    skipped = 0

    for record in records:
        total += 1

        if not yn_is_true(record.get("v_use_yn")):
            skipped += 1
            continue

        program_name = safe_filename(record.get("v_program_name"))
        if not program_name:
            skipped += 1
            continue

        out_path = out_dir / ("vrf_" + program_name)
        out_path.write_text(build_sql(record), encoding="utf-8")
        written += 1

    print("TOTAL_ROWS=%d WRITTEN=%d SKIPPED=%d OUT_DIR=%s" % (total, written, skipped, str(out_dir)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
