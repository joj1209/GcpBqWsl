#!/bin/bash
# BigQuery SQL 실행 스크립트 (variable substitution)
#
# list 파일 형식:
#   <sql_file.sql> <job_dt> <tbl_id>
# 예) bq_dw_red_care_sales_04.sql 20251223 DW.RED_CARE_SALES
#
# 치환 규칙:
#   {vs_pgm_id} : SQL 파일명(확장자 제외)
#   {vs_job_dt} : job_dt
#   {vs_tbl_id} : tbl_id

set -euo pipefail

BASE_DIR="/home/bskim/hc"

usage() {
  echo "Usage: $0 <sql.list>"
}

quote_bq_string() {
  local s="$1"
  s="${s//\'/\'\'}"
  printf "'%s'" "$s"
}

escape_sed_repl() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//&/\\&}"
  s="${s//|/\\|}"
  printf "%s" "$s"
}

run_one() {
  local sql_rel="$1"
  local job_dt="$2"
  local tbl_id="$3"

  local sql_file="$BASE_DIR/sql/$sql_rel"
  if [ ! -f "$sql_file" ]; then
    echo "[FAIL] File not found: $sql_file"
    return 1
  fi

  local pgm_id
  pgm_id="$(basename "$sql_rel" .sql)"

  local vs_pgm_id vs_job_dt vs_tbl_id
  vs_pgm_id="$(quote_bq_string "$pgm_id")"
  vs_job_dt="$(quote_bq_string "$job_dt")"
  vs_tbl_id="$(quote_bq_string "$tbl_id")"

  local tmp_sql
  tmp_sql="$(mktemp "${TMPDIR:-/tmp}/bqsql.XXXXXX.sql")"
  trap "rm -f '$tmp_sql'" RETURN

  sed \
    -e "s|{vs_pgm_id}|$(escape_sed_repl "$vs_pgm_id")|g" \
    -e "s|{vs_job_dt}|$(escape_sed_repl "$vs_job_dt")|g" \
    -e "s|{vs_tbl_id}|$(escape_sed_repl "$vs_tbl_id")|g" \
    "$sql_file" > "$tmp_sql"

  echo "[INFO] $sql_rel (job_dt=$job_dt, tbl_id=$tbl_id)"
  bq query --quiet --use_legacy_sql=false < "$tmp_sql"
  printf '\n'
}

if [ "$#" -ne 1 ]; then
  usage
  exit 1
fi

LIST_FILE="$1"
if [ ! -f "$LIST_FILE" ]; then
  echo "[FAIL] List file not found: $LIST_FILE"
  exit 1
fi

total=0
success=0
fail=0

while read -r line || [ -n "$line" ]; do
  line="$(echo "$line" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  [[ -z "$line" ]] && continue
  [[ "$line" =~ ^# ]] && continue

  sql_rel=""
  job_dt=""
  tbl_id=""
  IFS=$'\t ' read -r sql_rel job_dt tbl_id _rest <<< "$line"

  if [[ -z "$sql_rel" || -z "$job_dt" || -z "$tbl_id" ]]; then
    echo "[FAIL] Invalid list line: $line"
    fail=$((fail + 1))
    continue
  fi

  total=$((total + 1))
  if run_one "$sql_rel" "$job_dt" "$tbl_id"; then
    success=$((success + 1))
  else
    fail=$((fail + 1))
  fi

done < "$LIST_FILE"

echo "[SUMMARY] total=$total, success=$success, fail=$fail"
[ "$fail" -eq 0 ]
