#!/usr/bin/env python3
import sys
import subprocess
import os
import re

def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <sql_file_path>")
        sys.exit(1)

    sql_file = sys.argv[1]
    if not os.path.exists(sql_file):
        print(f"Error: File '{sql_file}' not found.")
        sys.exit(1)

    print(f"Reading SQL file: {sql_file}")
    with open(sql_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. DECLARE 구문 추출 (변수 선언부)
    # BEGIN 바로 다음부터 첫 번째 inner BEGIN 전까지의 DECLARE 문들을 찾습니다.
    declare_pattern = re.compile(r'DECLARE\s+.*?;', re.DOTALL | re.IGNORECASE)
    declares = declare_pattern.findall(content)
    declare_section = "\n".join(declares)

    print(f"Found {len(declares)} variable declarations.")

    # 2. 내부 BEGIN ... END 블록 추출
    # 정규식으로 inner BEGIN ... END; 블록들을 찾습니다.
    # 외곽의 BEGIN/END를 제외하고 내부 블록만 매칭합니다.
    block_pattern = re.compile(r'(\s*-- Block \d+\s+BEGIN\s+.*?\s+END;)', re.DOTALL | re.IGNORECASE)
    blocks = block_pattern.findall(content)

    if not blocks:
        # 주석이 없는 경우를 대비해 단순 BEGIN...END 매칭 시도 (조금 더 느슨하게)
        block_pattern = re.compile(r'(BEGIN\s+(?!DECLARE).*?END;)', re.DOTALL | re.IGNORECASE)
        blocks = [b for b in block_pattern.findall(content) if 'DECLARE' not in b]

    print(f"Found {len(blocks)} execution blocks.")
    print("-" * 50)

    # 3. 각 블록을 순차적으로 실행
    for i, block in enumerate(blocks, 1):
        print(f"\n[Executing Block {i}/{len(blocks)}]")
        
        # 실행할 쿼리 조합: 변수 선언부 + 해당 블록
        # 주의: 단일 블록 실행이므로 외곽의 BEGIN/END는 필요 없음 (변수 선언이 있으므로 스크립트로 인식됨)
        query_to_run = f"{declare_section}\n{block}"
        
        try:
            subprocess.run(
                ['bq', 'query', '--use_legacy_sql=false', '--format=pretty'],
                input=query_to_run,
                check=True,
                text=True
            )
        except subprocess.CalledProcessError:
            print(f"Block {i} failed. Stopping execution.")
            sys.exit(1)

    print("\n" + "-" * 50)
    print("All blocks executed successfully.")

if __name__ == "__main__":
    main()
