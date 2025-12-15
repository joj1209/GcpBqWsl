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

# 로그 디렉토리: /log/YYYYMMDD/
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

# bq 실행: 출력 전체를 임시 파일로 받고, 성공/실패에 따라 .log 또는 .log.err로 저장
bq query --use_legacy_sql=false < "$SQL_FILE" > "$TMP_LOG" 2>&1
exit_code=$?

# 일부 환경/상황에서 bq가 exit_code=0을 반환하더라도, 출력에 에러가 포함될 수 있어
# 로그 내용을 함께 검사하여 최종 성공/실패를 재판정합니다.
if [ $exit_code -eq 0 ] && grep -Eiq "(BigQuery error|Error in query operation|Error in query string|^FATAL|Syntax error:|Access Denied|Not found:|Invalid query|invalidQuery|Traceback)" "$TMP_LOG"; then
    exit_code=1
fi

# 최종 로그 파일 저장 및 종료 코드 반환
if [ $exit_code -eq 0 ]; then
    mv -f "$TMP_LOG" "$OUT_LOG"
    rm -f "$ERR_LOG"
    echo "Query executed successfully."
else
    mv -f "$TMP_LOG" "$ERR_LOG"
    rm -f "$OUT_LOG"
    echo "Query execution failed."
    echo "Error log saved to: $ERR_LOG"
fi

exit $exit_code
