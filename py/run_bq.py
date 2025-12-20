#!/usr/bin/env python3
import sys
import subprocess
import os

def main():
    # 파라미터 확인
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <sql_file_path>")
        sys.exit(1)

    sql_file = sys.argv[1]

    # 파일 존재 확인
    if not os.path.exists(sql_file):
        print(f"Error: File '{sql_file}' not found.")
        sys.exit(1)

    print(f"Running BigQuery script (Python): {sql_file}")
    print("-" * 40)

    try:
        # SQL 파일을 열어서 stdin으로 전달
        with open(sql_file, 'r', encoding='utf-8') as f:
            # bq 명령어 실행
            # shell=False가 보안상 안전하며, 리스트 형태로 명령어를 전달합니다.
            subprocess.run(
                ['bq', 'query', '--use_legacy_sql=false'],
                stdin=f,
                check=True,
                text=True
            )
    except subprocess.CalledProcessError as e:
        print(f"\nExecution failed with return code {e.returncode}")
        sys.exit(e.returncode)
    except FileNotFoundError:
        print("Error: 'bq' command not found. Please ensure Google Cloud SDK is in your PATH.")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
