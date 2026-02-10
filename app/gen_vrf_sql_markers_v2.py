#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Marker-based template generator (v2: compiled template, single-pass render).

What is improved vs v3:
- Uses only line markers (no inline marker) for robustness.
- Compiles template into (line markers + indent) once; render is a single pass.
- Uses regex-based scalar placeholder replacement ({{name}}) and validates no leftovers.
- Marker indentation controls output indentation (template decides formatting).

Template: app/vrf_template_markers_v2.sql
Markers (must exist exactly once each):
- --__METRICS_SELECT__       (line marker)
- --__WHERE_CLAUSE__         (line marker)
- --__METRICS_STRUCT__       (line marker)
- --__METRICS_JSON_SELECT__  (line marker)
- --__METRICS_JSON_SELECT_TXT__  (line marker)

Python 3.6 compatible.
"""

import argparse
import csv
import re
from pathlib import Path


_RE_PLACEHOLDER = re.compile(r"\{\{([A-Za-z_][A-Za-z0-9_]*)\}\}")


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


def _leading_ws(line):
    return line[: len(line) - len(line.lstrip(" \t"))]


class CompiledMarkerTemplate(object):
    def __init__(self, template_text, marker_names):
        self._lines = template_text.splitlines(True)  # keep endings
        self._marker_names = list(marker_names)

        marker_to_indexes = {}
        for i, line in enumerate(self._lines):
            for marker in self._marker_names:
                if marker in line:
                    marker_to_indexes.setdefault(marker, []).append(i)

        for marker in self._marker_names:
            idxs = marker_to_indexes.get(marker, [])
            if len(idxs) != 1:
                raise ValueError("Marker must appear exactly once: %s (found %d)" % (marker, len(idxs)))

        self._marker_index = {m: marker_to_indexes[m][0] for m in self._marker_names}
        self._marker_indent = {m: _leading_ws(self._lines[self._marker_index[m]]) for m in self._marker_names}

    def render(self, scalar_map, block_map):
        """Render template.

        scalar_map: {name: value} for {{name}} placeholders
        block_map: {marker: [line1, line2, ...]} where lines do NOT include newline
        """

        out_lines = []
        for line in self._lines:
            replaced = self._replace_scalars(line, scalar_map)

            marker_hit = None
            for marker in self._marker_names:
                if marker in replaced:
                    marker_hit = marker
                    break

            if marker_hit is None:
                out_lines.append(replaced)
                continue

            indent = self._marker_indent[marker_hit]
            block_lines = block_map.get(marker_hit) or []
            if not block_lines:
                continue

            for raw in block_lines:
                out_lines.append(indent + raw + "\n")

        rendered = "".join(out_lines)

        m = _RE_PLACEHOLDER.search(rendered)
        if m:
            raise ValueError("Unresolved placeholder: {{%s}}" % m.group(1))

        return rendered

    @staticmethod
    def _replace_scalars(text, scalar_map):
        def repl(match):
            name = match.group(1)
            if name not in scalar_map:
                return match.group(0)
            return scalar_map[name]

        return _RE_PLACEHOLDER.sub(repl, text)


def _chunked(items, chunk_size):
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def write_merged_sql(merge_dir, merge_prefix, rendered_items, merge_chunk):
    """Write merged sql files.

    rendered_items: [(program_name, sql_text), ...]
    merge_chunk: number of items per merged file
    """

    merge_chunk = int(merge_chunk)
    if merge_chunk <= 0:
        return 0

    merge_dir = Path(merge_dir)
    merge_dir.mkdir(parents=True, exist_ok=True)

    written = 0
    for idx, group in enumerate(_chunked(rendered_items, merge_chunk), start=1):
        out_path = merge_dir / ("%s_%04d.sql" % (merge_prefix, idx))
        parts = []
        for program_name, sql_text in group:
            parts.append("-- BEGIN %s\n" % program_name)
            parts.append(sql_text)
            if not sql_text.endswith("\n"):
                parts.append("\n")
            parts.append("-- END %s\n\n" % program_name)

        out_path.write_text("".join(parts), encoding="utf-8")
        written += 1

    return written


def build_metric_cols(record):
    metric_sum = yn_is_true(record.get("v_metric_sum_yn"))
    metric_cnt = yn_is_true(record.get("v_metric_cnt_yn"))

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

    return cnt_cols, sum_cols


def build_blocks(record):
    v_stat_dt = _strip(record.get("v_stat_dt"))

    cnt_cols, sum_cols = build_metric_cols(record)

    metrics_select_lines = []
    for col in cnt_cols:
        metrics_select_lines.append(", COUNT(DISTINCT %s) AS %s" % (col, col))
    for col in sum_cols:
        metrics_select_lines.append(", SUM(%s) AS %s" % (col, col))

    where_lines = []
    if yn_is_true(record.get("v_filter_yn")):
        filter_cols = []
        for key in ("v_filter_col1", "v_filter_col2"):
            col = normalize_identifier(record.get(key))
            if col:
                filter_cols.append(col)

        for i, col in enumerate(filter_cols):
            if i == 0:
                where_lines.append("WHERE %s = PARSE_DATE('%%Y%%m%%d', '%s')" % (col, v_stat_dt))
            else:
                where_lines.append("AND %s = PARSE_DATE('%%Y%%m%%d', '%s')" % (col, v_stat_dt))

    metrics_struct_lines = []
    metrics_json_select_lines = []

    metric_cols = cnt_cols + sum_cols
    if metric_cols:
        metrics_struct_lines.append(", STRUCT(")
        for idx, col in enumerate(metric_cols):
            comma = "," if idx < len(metric_cols) - 1 else ""
            metrics_struct_lines.append("  %s AS %s%s" % (col, col, comma))
        metrics_struct_lines.append(") AS METRICS")

        for col in metric_cols:
            raw = col
            if raw.startswith("`") and raw.endswith("`") and len(raw) >= 2:
                raw = raw[1:-1]
            raw_sql = raw.replace("'", "''")
            metrics_json_select_lines.append(", JSON_VALUE(STATS_CNT, '$[''METRICS''][''%s'']') AS %s" % (raw_sql, col))

    return metrics_select_lines, where_lines, metrics_struct_lines, metrics_json_select_lines


def render_one(template, record):
    mid = _strip(record.get("mid"))
    v_program_name = _strip(record.get("v_program_name"))
    v_stat_dt = _strip(record.get("v_stat_dt"))
    v_table_name_raw = _strip(record.get("v_table_name"))

    scalar_map = {
        "mid": mid,
        "v_program_name": v_program_name,
        "v_stat_dt": v_stat_dt,
        "v_table_name_raw": v_table_name_raw,
        "table_ref": normalize_identifier(v_table_name_raw),
    }

    metrics_select_lines, where_lines, metrics_struct_lines, metrics_json_select_lines = build_blocks(record)
    block_map = {
        "--__METRICS_SELECT__": metrics_select_lines,
        "--__WHERE_CLAUSE__": where_lines,
        "--__METRICS_STRUCT__": metrics_struct_lines,
        "--__METRICS_JSON_SELECT__": metrics_json_select_lines,
        "--__METRICS_JSON_SELECT_TXT__": metrics_json_select_lines,
    }

    return template.render(scalar_map, block_map)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Marker-based generator v2 (compiled template)")
    parser.add_argument("--table", default=str(Path("app") / "table.ini"), help="Input CSV")
    parser.add_argument(
        "--template",
        default=str(Path("app") / "vrf_template_markers_v2.sql"),
        help="Marker template file",
    )
    parser.add_argument("--out", default=str(Path("app") / "out"), help="Output directory")
    parser.add_argument(
        "--merge-dir",
        default=str(Path("app") / "out_merge"),
        help="Merged output directory (<=0 chunk disables)",
    )
    parser.add_argument(
        "--merge-prefix",
        default="vrf_merged",
        help="Merged output file prefix (files will be <prefix>_0001.sql ...)",
    )
    parser.add_argument(
        "--merge-chunk",
        type=int,
        default=100,
        help="How many generated SQLs to merge into one file (<=0 disables merge)",
    )
    args = parser.parse_args(argv)

    table_path = Path(args.table)
    template_path = Path(args.template)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    template_text = template_path.read_text(encoding="utf-8", errors="replace")
    template = CompiledMarkerTemplate(
        template_text,
        marker_names=[
            "--__METRICS_SELECT__",
            "--__WHERE_CLAUSE__",
            "--__METRICS_STRUCT__",
            "--__METRICS_JSON_SELECT__",
            "--__METRICS_JSON_SELECT_TXT__",
        ],
    )

    records = read_table_ini(table_path)

    total = 0
    written = 0
    skipped = 0
    rendered_items = []

    for record in records:
        total += 1

        if not yn_is_true(record.get("v_use_yn")):
            skipped += 1
            continue

        program_name = safe_filename(record.get("v_program_name"))
        if not program_name:
            skipped += 1
            continue

        sql_text = render_one(template, record)

        out_path = out_dir / ("vrf_" + program_name)
        out_path.write_text(sql_text, encoding="utf-8")
        rendered_items.append(("vrf_" + program_name, sql_text))
        written += 1

    merged_files = write_merged_sql(args.merge_dir, args.merge_prefix, rendered_items, args.merge_chunk)

    print(
        "TOTAL_ROWS=%d WRITTEN=%d SKIPPED=%d OUT_DIR=%s MERGED_FILES=%d MERGE_DIR=%s MERGE_CHUNK=%d"
        % (total, written, skipped, str(out_dir), merged_files, str(Path(args.merge_dir)), int(args.merge_chunk))
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
