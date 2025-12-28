# run_bq_var_json.py 변경사항 정리 (2025-12-28)

본 문서는 `py/run_bq_var_json.py`에 반영된 신규 요구사항 및 구현 내용을 정리합니다.

## 1) 목표/요구사항

원 요구사항(요약):

1. 대상 프로그램: `py/run_bq_var_json.py`
2. `src/list/bq.csv`를 읽어서 JSON 기준정보 파일을 만든다(헤더가 키).
3. 생성된 JSON을 기준정보로 읽고, 실행 시 케이스별로 필터/오버라이드하여 실행한다.
   - 공통 실행 대상: `use_yn=Y`
   - 아규먼트가 없는 값은 JSON 값을 사용
   - 케이스
     - case1: `mid=qa` → mid=qa인 SQL 전부 실행
     - case2: `vs_pgm_id=bq_dw_red_care_sales_01.sql` → 1개만 실행
     - case3: `vs_pgm_id=... vs_job_dt=20251201` → 1개만 실행 + job_dt만 오버라이드
     - case4: `vs_job_dt=20251202` → 전체 실행 + job_dt만 오버라이드

추가 요청:
- `run_bq_var_log.py`처럼 `logger` 기반 로깅 사용
- 로그 파일도 생성

## 2) 변경된 파일

- `py/run_bq_var_json.py`
- `src/list/bq.csv` (선택: `use_yn` 컬럼 추가 가능)
- `src/list/bq.json` (매 실행 시 재생성되는 기준정보)

## 3) 실행 흐름

`py/run_bq_var_json.py`는 아래 순서로 동작합니다.

1. `src/list/bq.csv`를 읽어 레코드 목록(dict list)을 생성
2. 위 레코드를 `src/list/bq.json`으로 저장 (기준정보 생성)
3. `src/list/bq.json`을 다시 로딩 (기준정보 사용)
4. 공통 필터: `use_yn=Y`만 대상
   - `use_yn` 컬럼이 **없으면 전체를 Y로 간주**
5. CLI 인자로 추가 필터 적용
   - `mid=<값>` 제공 시: 해당 `mid`만
   - `vs_pgm_id=<sql파일명>` 제공 시: 해당 `vs_pgm_id`만
6. CLI 인자로 오버라이드 적용
   - `mid`, `vs_pgm_id`를 제외한 인자들은 각 레코드의 값을 덮어씀
   - 예: `vs_job_dt=20251202` → 실행 대상 전체 레코드에 job_dt=20251202 적용
7. 각 대상 레코드별로 SQL 파일을 읽어 템플릿 치환 후 `bq query` 실행

## 4) 입력 파일 포맷

### 4.1 src/list/bq.csv

- 첫 줄(헤더)은 콤마(,) 구분이며, 이 값들이 JSON 키가 됩니다.
- 데이터 줄은 두 가지 스타일 모두 지원합니다.

#### (권장) 정석 CSV(콤마로 모든 컬럼 구분)

```csv
mid,vs_pgm_id,vs_job_dt,vs_tbl_id,use_yn
qa,bq_dw_red_care_sales_01.sql,20251223,DW.RED_CARE_SALES,Y
qb,bq_dw_red_care_sales_02.sql,20251223,DW.RED_CARE_SALES,N
```

#### (허용) 기존 파일 스타일(첫 콤마 + 나머지 공백 혼용)

헤더가 5개라면, 아래처럼 마지막 필드까지 공백으로 나눠 써도 처리됩니다.

```text
mid,vs_pgm_id,vs_job_dt,vs_tbl_id,use_yn
qa,bq_dw_red_care_sales_01.sql 20251223 DW.RED_CARE_SALES Y
```

> 주의: 헤더 개수에 맞춰 최종적으로 필드 수가 정확히 맞아야 합니다.

### 4.2 src/list/bq.json

- `bq.csv`에서 읽은 레코드를 그대로 JSON 배열로 저장합니다.
- 예:

```json
[
  {
    "mid": "qa",
    "vs_pgm_id": "bq_dw_red_care_sales_01.sql",
    "vs_job_dt": "20251223",
    "vs_tbl_id": "DW.RED_CARE_SALES",
    "use_yn": "Y"
  }
]
```

## 5) 실행 케이스 예시

아래는 요구사항 케이스 기준 예시입니다.

- case1: `mid=qa`만 실행
  - `python3 py/run_bq_var_json.py mid=qa`

- case2: 특정 SQL 1개만 실행
  - `python3 py/run_bq_var_json.py vs_pgm_id=bq_dw_red_care_sales_01.sql`

- case3: 특정 SQL 1개만 실행 + `vs_job_dt` 오버라이드
  - `python3 py/run_bq_var_json.py vs_pgm_id=bq_dw_red_care_sales_01.sql vs_job_dt=20251201`

- case4: 전체 실행 + `vs_job_dt` 오버라이드
  - `python3 py/run_bq_var_json.py vs_job_dt=20251202`

공통 규칙:
- 대상 레코드는 `use_yn=Y`인 행만 실행
- 인자 없는 값은 JSON 값이 사용됨

## 6) 로깅(콘솔 + 파일)

### 6.1 콘솔 로깅

- `run_bq_var_log.py`와 유사한 포맷 사용
- 포맷: `%(asctime)s [%(levelname)s] %(message)s`

### 6.2 로그 파일 생성

실행 시 날짜 디렉토리를 생성하고 로그 파일 2개를 남깁니다.

- 디렉토리: `log/YYYYMMDD/`
- 성공/일반 로그: `run_bq_var_json.HHMMSS.PID.log`
- 에러 로그: `run_bq_var_json.HHMMSS.PID.log.err`

분리 규칙:
- `.log`에는 INFO/WARNING까지만 기록
- `.log.err`에는 ERROR 이상만 기록

## 7) SQL 템플릿 치환 규칙

SQL 파일(`sql/*.sql`) 내부의 아래 플레이스홀더를 문자열 리터럴로 치환합니다.

- `{vs_pgm_id}` → 파일명 stem (예: `bq_dw_red_care_sales_01`)
- `{vs_job_dt}` → 레코드(또는 오버라이드)의 `vs_job_dt`
- `{vs_tbl_id}` → 레코드(또는 오버라이드)의 `vs_tbl_id`

문자열은 BigQuery 규칙에 맞게 single-quote로 감싸며, 내부 single-quote는 `''`로 escape합니다.

## 8) 참고/운영 팁

- `mid=__none__` 같이 매칭되지 않는 값을 주면, JSON은 생성되지만 실행 대상이 없어 `bq` 호출 없이 종료됩니다(스모크 테스트에 유용).
- `bq` CLI가 PATH에 있어야 실제 실행이 가능합니다.
