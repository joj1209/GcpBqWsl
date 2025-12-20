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

echo "Running BigQuery script: $SQL_FILE"
echo "----------------------------------------"

# bq 실행
bq query --use_legacy_sql=false < "$SQL_FILE"
exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo "Query executed successfully."
else
    echo "Query execution failed."
fi

exit $exit_code
