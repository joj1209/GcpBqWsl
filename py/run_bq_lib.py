import sys
import os
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

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

    print(f"Running BigQuery script (Python Library): {sql_file}")
    print("-" * 40)

    try:
        # BQ 클라이언트 초기화
        client = bigquery.Client()

        # SQL 파일 내용 읽기
        with open(sql_file, "r", encoding='utf-8') as f:
            sql_script = f.read()

        # 쿼리 작업 실행
        job = client.query(sql_script)

        # 작업 완료 대기
        result = job.result() 

        print(f"BigQuery 스크립트 작업이 완료되었습니다. 작업 ID: {job.job_id}")
        
        # 결과 출력
        rows = list(result)
        if rows:
            print(f"\nResult Rows: {len(rows)}")
            for row in rows:
                print(dict(row))
        else:
            print("\nNo result rows returned (or script execution completed).")

    except GoogleAPIError as e:
        print(f"\nBigQuery API Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
