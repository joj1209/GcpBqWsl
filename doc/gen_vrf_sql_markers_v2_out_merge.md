# gen_vrf_sql_markers_v2.py 변경사항 (out_merge 병합 생성)

## 목적
기존에는 `app/out/` 아래에 프로그램별 SQL 파일(`vrf_<program_name>`)만 생성했습니다.
이번 변경으로 **생성된 개별 SQL들을 chunk 단위로 병합한 파일을 `app/out_merge/`에도 추가로 생성**합니다.

- 개별 파일 생성: 유지 (기존 동작 그대로)
- 병합 파일 생성: 신규
- 병합 개수(chunk): CLI 옵션으로 조정 가능


## 변경된 파일
- `app/gen_vrf_sql_markers_v2.py`


## 신규 CLI 옵션
아래 옵션들이 `gen_vrf_sql_markers_v2.py`에 추가되었습니다.

### `--merge-dir`
- 기본값: `app/out_merge`
- 의미: 병합 결과 파일이 생성될 디렉터리

### `--merge-prefix`
- 기본값: `vrf_merged`
- 의미: 병합 결과 파일 prefix
- 실제 파일명 규칙: `<merge-prefix>_0001.sql`, `<merge-prefix>_0002.sql`, ...

### `--merge-chunk`
- 기본값: `100`
- 의미: 병합 파일 1개에 포함될 “개별 생성 SQL” 개수
- 비활성화: `--merge-chunk 0` 또는 음수 지정 시 **병합 파일 생성 안 함**


## 출력 디렉터리 및 파일 규칙

### 1) 개별 생성 파일 (기존과 동일)
- 디렉터리: `--out` (기본 `app/out`)
- 파일명: `vrf_<safe_program_name>`
  - `safe_program_name`: `/`, `\\`, `:` 문자를 `_` 로 치환

### 2) 병합 파일 (신규)
- 디렉터리: `--merge-dir` (기본 `app/out_merge`)
- 파일명: `<merge-prefix>_NNNN.sql`
  - NNNN: 4자리 0-padding 순번
  - 예: `vrf_merged_0001.sql`


## 병합 파일 포맷
병합 파일은 여러 개의 개별 SQL 텍스트를 이어 붙인 형태이며, 각 블록은 아래 주석으로 경계가 표시됩니다.

- 시작: `-- BEGIN <program_name>`
- 종료: `-- END <program_name>`

여기서 `<program_name>`은 개별 파일명과 동일하게 `vrf_<safe_program_name>` 형태로 기록됩니다.

예시(개념):

```sql
-- BEGIN vrf_xxx.sql
<sql_text>
-- END vrf_xxx.sql

-- BEGIN vrf_yyy.sql
<sql_text>
-- END vrf_yyy.sql
```

추가 규칙:
- 개별 SQL 텍스트가 newline(`\n`)로 끝나지 않으면 병합 시 자동으로 newline을 보정합니다.


## 실행 예시

### 기본 실행(개별 + 병합 생성)
```bash
python3 app/gen_vrf_sql_markers_v2.py
```

### 병합 파일을 50개 단위로 생성
```bash
python3 app/gen_vrf_sql_markers_v2.py --merge-chunk 50
```

### 병합 디렉터리/파일 prefix 변경
```bash
python3 app/gen_vrf_sql_markers_v2.py --merge-dir app/out_merge2 --merge-prefix vrf_all
```

### 병합 기능 끄기 (개별 파일만 생성)
```bash
python3 app/gen_vrf_sql_markers_v2.py --merge-chunk 0
```


## 내부 동작 요약
변경된 main 루프 동작은 아래와 같습니다.

1. `table.ini`를 읽어서 `v_use_yn == Y` 인 row만 처리
2. 각 row마다:
   - `render_one(...)`으로 SQL 텍스트 생성
   - 개별 파일로 저장: `app/out/vrf_<program_name>`
   - (신규) 메모리에 `(file_name, sql_text)`를 누적
3. 루프 종료 후:
   - (신규) 누적된 결과들을 `--merge-chunk` 단위로 잘라 `--merge-dir`에 병합 파일로 저장


## 출력 로그 변경
기존:
- `TOTAL_ROWS=... WRITTEN=... SKIPPED=... OUT_DIR=...`

변경 후:
- 위 항목 + `MERGED_FILES`, `MERGE_DIR`, `MERGE_CHUNK`가 추가됩니다.

예:

```text
TOTAL_ROWS=4 WRITTEN=4 SKIPPED=0 OUT_DIR=app/out MERGED_FILES=2 MERGE_DIR=app/out_merge MERGE_CHUNK=2
```


## 참고 / 주의사항
- 병합 파일은 단순 concatenation이므로, SQL 실행 방식(예: BigQuery CLI에서 multi-statement 허용 여부)에 따라 실행 전략이 달라질 수 있습니다.
- 병합 파일 경계 주석(`-- BEGIN/END`)은 사람이 보기 쉽도록 넣은 것으로, 실행 엔진이 이 주석을 무시하는지 확인이 필요합니다(대부분 무시됨).
