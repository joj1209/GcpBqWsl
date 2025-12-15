#!/bin/bash

# 사용법 확인
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <sql_file_path>"
    exit 1
fi

SQL_FILE="$1"

# 파일 존재 확인
if [ ! -f "$SQL_FILE" ]; then
    echo "Error: File '$SQL_FILE' not found."
    exit 1
fi

# 로그 디렉토리: /log/YYYYMMDD/ (프로젝트 루트의 log 폴더)
RUN_DATE="$(date +%Y%m%d)"
LOG_DIR="log/$RUN_DATE"
mkdir -p "$LOG_DIR"

# 로그 파일명: 실행파일명(sql 대신 log)
SQL_BASENAME="$(basename -- "$SQL_FILE")"
SQL_NAME_NO_EXT="${SQL_BASENAME%.*}"
OUT_LOG="$LOG_DIR/${SQL_NAME_NO_EXT}.log"
ERR_LOG="$LOG_DIR/${SQL_NAME_NO_EXT}.log.err"
TMP_LOG="$LOG_DIR/.${SQL_NAME_NO_EXT}.log.tmp"

echo "Running BigQuery script: $SQL_FILE"
echo "LOG_DIR     : $LOG_DIR"
echo "SUCCESS LOG : $OUT_LOG"
echo "ERROR LOG   : $ERR_LOG"
echo "----------------------------------------"

# bq 실행: stdout/stderr가 섞여 나오는 경우가 있어서, 항상 합쳐서 임시 파일에 저장한 뒤
# 성공이면 .log, 실패이면 .log.err로 저장합니다.
bq query --use_legacy_sql=false < "$SQL_FILE" > "$TMP_LOG" 2>&1
exit_code=$?

if [ $exit_code -eq 0 ]; then
    mv -f "$TMP_LOG" "$OUT_LOG"
    rm -f "$ERR_LOG"
    echo "Query executed successfully."
    exit 0
else
    mv -f "$TMP_LOG" "$ERR_LOG"
    rm -f "$OUT_LOG"
    echo "Query execution failed."
    echo "Error log saved to: $ERR_LOG"
    exit $exit_code
fi
