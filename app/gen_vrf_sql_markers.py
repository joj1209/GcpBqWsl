#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Marker-based template SQL generator.

This approach aims to combine:
- template-based advantage: keep shape/format of SQL in a single template file
- static-generation stability: inject only into explicit, well-defined locations

Template: app/vrf_template_markers.sql

Scalar placeholders:
  - Use {{name}} and replace literally.

Explicit markers:
  - Line marker:   --__METRICS_SELECT__   -> replaced with 0..N metric SELECT lines (or removed)
  - Line marker:   --__WHERE_CLAUSE__     -> replaced with 0..N WHERE/AND lines (or removed)
  - Inline marker: --__METRICS_STRUCT__   -> replaced with ',\n    STRUCT(...) AS METRICS' or ''

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
    return s.replace("/", "_").replace("\\", "_").replace(":", "_")


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


def replace_scalar_placeholders(text, mapping):
    for k, v in mapping.items():
        text = text.replace("{{%s}}" % k, v)
    return text


def replace_line_marker(text, marker, replacement_text):
    """Replace the entire line that contains marker.

    - If replacement_text is '', the marker line is removed.
    - replacement_text should include trailing newline if it spans multiple lines.
    """

    lines = text.splitlines(True)  # keep line endings
    out = []
    found = 0

    for line in lines:
        if marker in line:
            found += 1
            if replacement_text:
                out.append(replacement_text)
        else:
            out.append(line)

    if found != 1:
        raise ValueError("Expected exactly 1 marker line for %s, found %d" % (marker, found))

    return "".join(out)


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
        metrics_select = "\n".join(metrics_select_lines) + "\n"

    where_clause = ""
    if filter_cols:
        v_stat_dt = _strip(record.get("v_stat_dt"))
        where_lines = []
        for i, col in enumerate(filter_cols):
            prefix = "WHERE" if i == 0 else "  AND"
            where_lines.append(
                "   %s %s = PARSE_DATE('%%Y%%m%%d', '%s')" % (prefix, col, v_stat_dt)
            )
        where_clause = "\n".join(where_lines) + "\n"

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

    text = replace_scalar_placeholders(
        template_text,
        {
            "mid": mid,
            "v_program_name": v_program_name,
            "v_stat_dt": v_stat_dt,
            "v_table_name_raw": v_table_name_raw,
            "table_ref": table_ref,
        },
    )

    metrics_select, where_clause, metrics_struct = build_blocks(record)

    text = replace_line_marker(text, "--__METRICS_SELECT__", metrics_select)
    text = replace_line_marker(text, "--__WHERE_CLAUSE__", where_clause)

    if text.count("--__METRICS_STRUCT__") != 1:
        raise ValueError("Expected exactly 1 inline marker --__METRICS_STRUCT__")
    text = text.replace("--__METRICS_STRUCT__", metrics_struct)

    return text


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Marker-based generator for app/out/vrf_*.sql (explicit marker substitution)"
    )
    parser.add_argument(
        "--table",
        default=str(Path("app") / "table.ini"),
        help="Input CSV (default: app/table.ini)",
    )
    parser.add_argument(
        "--template",
        default=str(Path("app") / "vrf_template_markers.sql"),
        help="Template file (default: app/vrf_template_markers.sql)",
    )
    parser.add_argument(
        "--out",
        default=str(Path("app") / "out"),
        help="Output directory (default: app/out)",
    )
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

    print(
        "TOTAL_ROWS=%d WRITTEN=%d SKIPPED=%d OUT_DIR=%s"
        % (total, written, skipped, str(out_dir))
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
