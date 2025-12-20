#!/bin/bash
# BigQuery SQL 파일 실행 스크립트 (after)

# 사용법 확인
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <sql_file.sql | sql.list>"
    exit 1
fi

BASE_DIR="/home/bskim/hc"

usage() {
  echo "Usage: $0 <sql_file.sql | sql.list>"
}

# -----------------------------------
# 단일 SQL 실행
# -----------------------------------
run_sql_file() {
  local sql_file=$BASE_DIR/sql/"$1"

  if [ ! -f "$sql_file" ]; then
    echo "[FAIL] File not found: $sql_file"
    return 1
  fi

  echo "[INFO] Running BigQuery script: $sql_file"
  echo "----------------------------------------"

  # In some environments, `bq` ends its progress/status output without a trailing newline.
  # That can make it look like the script is stuck until you press Enter (prompt appears on same line).
  # `--quiet` also avoids interactive confirmations.
  bq query --quiet --use_legacy_sql=false < "$sql_file"
  local exit_code=$?
  printf '\n'

  if [ $exit_code -eq 0 ]; then
    echo "[OK]   Success: $sql_file"
    return 0
  else
    echo "[FAIL] Failed : $sql_file (exit_code=$exit_code)"
    return $exit_code
  fi
}

# -----------------------------------
# list 파일 실행
# -----------------------------------
run_list_file() {
  local list_file="$1"

  if [ ! -f "$list_file" ]; then
    echo "[FAIL] List file not found: $list_file"
    return 1
  fi

  echo "[INFO] Running SQL list: $list_file"
  echo "----------------------------------------"

  local total=0 success=0 fail=0
  local failed_files=()

  while IFS= read -r line || [ -n "$line" ]; do
    # 앞뒤 공백 제거
    local sql_file
    sql_file="$(echo "$line" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"

    # ✅ PASS 조건
    [[ -z "$sql_file" ]] && continue        # 빈 줄
    [[ "$sql_file" =~ ^# ]] && continue     # 주석(#)

    total=$((total + 1))

    if run_sql_file "$sql_file"; then
      success=$((success + 1))
    else
      fail=$((fail + 1))
      failed_files+=("$sql_file")
    fi

    echo "----------------------------------------"
  done < "$list_file"

  echo "[SUMMARY] total=$total, success=$success, fail=$fail"

  if [ $fail -ne 0 ]; then
    echo "[SUMMARY] Failed files:"
    for f in "${failed_files[@]}"; do
      echo "  - $f"
    done
    return 1
  fi

  return 0
}

# -----------------------------------
# 메인
# -----------------------------------
INPUT="$1"

case "$INPUT" in
  *.sql)
    run_sql_file "$INPUT"
    exit $?
    ;;
  *.list)
    run_list_file "$INPUT"
    exit $?
    ;;
  *)
    echo "[ERROR] Unsupported file type: $INPUT"
    usage
    exit 1
    ;;
esac
