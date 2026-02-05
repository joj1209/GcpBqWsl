#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Template-based SQL generator.

- Input: app/table.ini (CSV)
- Template: app/vrf_template.sql (string.Template)
- Output: app/out/vrf_<v_program_name>

Rules (doc/요구사항_20260225.txt):
- v_use_yn!=Y => skip
- v_metric_sum_yn=Y => include SUM metrics for non-empty v_sum_col1/v_sum_col2
- v_metric_cnt_yn=Y => include COUNT(DISTINCT) metrics for non-empty v_cnt_col1/v_cnt_col2
- v_filter_yn=Y => include WHERE filters for non-empty v_filter_col1/v_filter_col2

Python 3.6 compatible.
"""

import argparse
import csv
from pathlib import Path
from string import Template


def yn_is_true(value):
    return (value or "").strip().upper() == "Y"


def _strip(value):
    return (value or "").strip()


def normalize_identifier(value):
    """Quote identifier with backticks unless it's already quoted."""
    s = _strip(value)
    if not s:
        return ""
    if "`" in s:
        return s
    return "`%s`" % s


def safe_filename(name):
    s = _strip(name)
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


def build_blocks(record):
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

    metrics_select_lines = []
    for col in cnt_cols:
        metrics_select_lines.append("       , COUNT(DISTINCT %s) AS %s" % (col, col))
    for col in sum_cols:
        metrics_select_lines.append("       , SUM(%s) AS %s" % (col, col))

    metrics_select = ""
    if metrics_select_lines:
        metrics_select = "\n".join(metrics_select_lines)

    where_clause = ""
    if filter_cols:
        v_stat_dt = _strip(record.get("v_stat_dt"))
        where_lines = []
        for i, col in enumerate(filter_cols):
            prefix = "WHERE" if i == 0 else "  AND"
            where_lines.append(
                "   %s %s = PARSE_DATE('%%Y%%m%%d', '%s')" % (prefix, col, v_stat_dt)
            )
        where_clause = "\n" + "\n".join(where_lines)

    metrics_struct = ""
    if cnt_cols or sum_cols:
        struct_lines = []
        for col in cnt_cols + sum_cols:
            struct_lines.append("      %s AS %s" % (col, col))
        metrics_struct = ",\n    STRUCT(\n%s\n    ) AS METRICS" % ",\n".join(struct_lines)

    return metrics_select, where_clause, metrics_struct


def render_sql(template_text, record):
    mid = _strip(record.get("mid"))
    v_program_name = _strip(record.get("v_program_name"))
    v_stat_dt = _strip(record.get("v_stat_dt"))
    v_table_name_raw = _strip(record.get("v_table_name"))

    table_ref = normalize_identifier(v_table_name_raw)

    metrics_select, where_clause, metrics_struct = build_blocks(record)

    data = {
        "mid": mid,
        "v_program_name": v_program_name,
        "v_stat_dt": v_stat_dt,
        "v_table_name_raw": v_table_name_raw,
        "table_ref": table_ref,
        "metrics_select": metrics_select,
        "where_clause": where_clause,
        "metrics_struct": metrics_struct,
    }

    return Template(template_text).safe_substitute(data)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Template-based generator for app/out/vrf_*.sql")
    parser.add_argument("--table", default=str(Path("app") / "table.ini"), help="Input CSV (default: app/table.ini)")
    parser.add_argument("--template", default=str(Path("app") / "vrf_template.sql"), help="Template file (default: app/vrf_template.sql)")
    parser.add_argument("--out", default=str(Path("app") / "out"), help="Output directory (default: app/out)")
    args = parser.parse_args(argv)

    table_path = Path(args.table)
    template_path = Path(args.template)
    out_dir = Path(args.out)

    out_dir.mkdir(parents=True, exist_ok=True)

    template_text = template_path.read_text(encoding="utf-8", errors="replace")

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
        out_path.write_text(render_sql(template_text, record), encoding="utf-8")
        written += 1

    print("TOTAL_ROWS=%d WRITTEN=%d SKIPPED=%d OUT_DIR=%s" % (total, written, skipped, str(out_dir)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
