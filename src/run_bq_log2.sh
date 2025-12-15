#!/usr/bin/env bash
set -o nounset
set -o pipefail

usage() {
  echo "Usage: $(basename "$0") <sql_file_path>"
}

die() {
  echo "Error: $*" >&2
  exit 1
}

# ---- args / precheck ---------------------------------------------------------
[[ $# -eq 1 ]] || { usage; exit 1; }

SQL_FILE="$1"
[[ -f "$SQL_FILE" ]] || die "File '$SQL_FILE' not found."

command -v bq >/dev/null 2>&1 || die "'bq' command not found. Install/initialize Google Cloud SDK."

# ---- paths -------------------------------------------------------------------
RUN_DATE="$(date +%Y%m%d)"
LOG_DIR="log/${RUN_DATE}"
mkdir -p "$LOG_DIR" || die "Failed to create log directory: $LOG_DIR"

SQL_BASENAME="$(basename -- "$SQL_FILE")"
SQL_NAME_NO_EXT="${SQL_BASENAME%.*}"

OUT_LOG="${LOG_DIR}/${SQL_NAME_NO_EXT}.log"
ERR_LOG="${LOG_DIR}/${SQL_NAME_NO_EXT}.log.err"
TMP_LOG="${LOG_DIR}/.${SQL_NAME_NO_EXT}.log.tmp"

# ---- helpers -----------------------------------------------------------------
ERROR_PATTERN='(BigQuery error|Error in query operation|Error in query string|^FATAL|Syntax error:|Access Denied|Not found:|Invalid query|invalidQuery|Traceback)'

has_error_in_output() {
  local file="$1"
  grep -Eiq "$ERROR_PATTERN" "$file"
}

finalize_logs() {
  local exit_code="$1"

  if [[ $exit_code -eq 0 ]]; then
    mv -f "$TMP_LOG" "$OUT_LOG"
    rm -f "$ERR_LOG" 2>/dev/null || true
    echo "Query executed successfully."
  else
    mv -f "$TMP_LOG" "$ERR_LOG"
    rm -f "$OUT_LOG" 2>/dev/null || true
    echo "Query execution failed."
    echo "Error log saved to: $ERR_LOG"
  fi

  exit "$exit_code"
}

cleanup() {
  # 중간에 중단되면 tmp가 남지 않도록 정리
  [[ -f "$TMP_LOG" ]] && rm -f "$TMP_LOG"
}
trap cleanup INT TERM

# ---- run ---------------------------------------------------------------------
echo "Running BigQuery script: $SQL_FILE"
echo "LOG_DIR     : $LOG_DIR"
echo "SUCCESS LOG : $OUT_LOG"
echo "ERROR LOG   : $ERR_LOG"
echo "----------------------------------------"

# bq 실행(표준출력/에러를 한 파일에 모음)
bq query --use_legacy_sql=false < "$SQL_FILE" >"$TMP_LOG" 2>&1
exit_code=$?

# exit_code=0이어도 출력에 에러 문구가 있으면 실패로 재판정
if [[ $exit_code -eq 0 ]] && has_error_in_output "$TMP_LOG"; then
  exit_code=1
fi

finalize_logs "$exit_code"
