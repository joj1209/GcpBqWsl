# run_bq_var_log.py 학습용 해설서 (Python 3.6)

대상 스크립트: [src/run_bq_var_log.py](../src/run_bq_var_log.py)

이 문서는 **공부/실력향상**을 위해, `run_bq_var_re.py` 기반으로 만든 `run_bq_var_log.py`를 **실행 예시 포함 + 라인(구간)별로** 설명합니다.

## 1) 이 스크립트가 하는 일(요약)

- 입력: `sql.list` 파일 (각 줄: `<sql파일경로> <job_dt> <tbl_id>`)
- 처리:
  - 각 줄을 파싱해서 SQL 파일을 읽음
  - SQL 템플릿의 변수 `{vs_pgm_id}`, `{vs_job_dt}`, `{vs_tbl_id}`를 문자열로 치환
  - `bq query --use_legacy_sql=false`로 BigQuery 실행
- 출력: `print` 대신 **표준 `logging`**으로 진행 로그/오류/요약을 기록

## 2) 사전 준비(실행 전 체크)

- Python: 3.6 이상 (이 스크립트는 3.6 문법만 사용)
- `bq` CLI가 PATH에 있어야 함
- `bq query`가 동작하도록 GCP 인증/권한이 준비되어 있어야 함

## 3) 실행 예시

### 3.0 실습 파일을 실제로 만들기(추천)

아래는 **직접 파일을 생성해서 끝까지 실행**해보는 실습 절차입니다.

1) 프로젝트 루트로 이동

```bash
cd /home/bskim/hc
```

2) 샘플 SQL 템플릿 생성 (최소 예제)

- 파일: `sql/sample.sql`

```bash
cat > sql/sample.sql <<'SQL'
-- minimal sample
SELECT
  {vs_pgm_id} AS pgm_id,
  {vs_job_dt} AS job_dt,
  {vs_tbl_id} AS tbl_id;
SQL
```

3) list 파일 생성

- 파일: `src/list/bq_sample.list`

```bash
cat > src/list/bq_sample.list <<'LIST'
# 주석/빈 줄은 무시됨
sample.sql 20251225 my_table
LIST
```

4) 실행

```bash
python3 src/run_bq_var_log.py src/list/bq_sample.list
```

또는 `.sql` 단일 실행:

```bash
python3 src/run_bq_var_log.py sql/sample.sql
```

> 참고: 환경에 따라 `bq query` 실행 권한/프로젝트 설정이 필요합니다.

### 3.0.1 `.sql` 파일 1개만 바로 실행하기

`run_bq_var_log.py`는 입력 파일 확장자에 따라 동작이 다릅니다.

- `.list` : 여러 SQL을 파라미터 치환 후 실행(기존 방식)
- `.sql` : SQL 파일 **1개를 그대로** 실행(파라미터 치환 없음)

예시:

```bash
python3 src/run_bq_var_log.py sql/sample.sql
```

주의:
- `.sql` 모드에서는 `{vs_job_dt}` 같은 템플릿 변수를 **치환하지 않습니다**.
- SQL에 `{vs_pgm_id}`, `{vs_job_dt}`, `{vs_tbl_id}`가 남아있으면 실행 전에 **WARNING 로그**를 출력합니다.


### 3.1 list 파일 예시

예를 들어, 다음 내용을 가진 파일을 하나 만듭니다.

- 파일: `src/list/bq_sample.list`

```text
# 주석/빈 줄은 무시됨
sample.sql 20251225 my_table
bq_dw_red_care_sales_01.sql 20251225 sales_table
```

의미:
- `sample.sql`을 실행하면서
  - `{vs_job_dt}` ← `'20251225'`
  - `{vs_tbl_id}` ← `'my_table'`
  - `{vs_pgm_id}` ← `'sample'` (파일명 stem)

### 3.2 실행 명령

프로젝트 루트(`/home/bskim/hc`)에서:

```bash
python3 src/run_bq_var_log.py src/list/bq_sample.list
```

또는 `.sql` 단일 실행:

```bash
python3 src/run_bq_var_log.py sql/sample.sql
```

(회사 환경이 `python`이 3.6이라면 `python src/run_bq_var_log.py ...` 형태로 실행해도 됩니다.)

### 3.3 출력 예시(형태)

성공 시 대략 이런 형태입니다:

```text
2025-12-25 10:30:00,123 [INFO] sample.sql (job_dt=20251225, tbl_id=my_table)
2025-12-25 10:30:02,456 [INFO] bq_dw_red_care_sales_01.sql (job_dt=20251225, tbl_id=sales_table)
2025-12-25 10:30:05,789 [INFO] SUMMARY total=2, success=2, fail=0
```

실패 시(예: SQL 파일이 없거나 bq 실행 실패):

```text
2025-12-25 10:30:00,123 [ERROR] SQL file not found: /home/bskim/hc/sql/not_exists.sql
2025-12-25 10:30:00,456 [ERROR] bq query failed (exit_code=1)
2025-12-25 10:30:00,789 [INFO] SUMMARY total=1, success=0, fail=1
```

## 4) 라인(구간)별 상세 설명

아래 설명은 [src/run_bq_var_log.py](../src/run_bq_var_log.py) 기준입니다.

### 4.1 1~7행: Shebang + import

- (1) `#!/usr/bin/env python3`
  - 이 파일을 실행 파일처럼(`./...`) 실행할 때 사용할 파이썬 인터프리터를 지정합니다.
- (2) `import logging`
  - `print()` 대신 사용할 표준 로거.
- (3) `import re`
  - list 파일의 각 줄을 공백 기준으로 쪼갤 때 정규식을 사용.
- (4) `import subprocess`
  - 외부 명령(`bq query`)을 실행.
- (5) `import sys`
  - 커맨드라인 인자(`sys.argv`) 처리.
- (6) `from pathlib import Path`
  - 경로 처리를 문자열 대신 객체로 안전하게 처리.
- (7) `from typing import NamedTuple, Optional`
  - `NamedTuple`: dataclasses 없이 “불변 레코드”를 만드는 대안.
  - `Optional[T]`: `T 또는 None` 타입 표현(Python 3.6 호환).

### 4.2 9행: 모듈 로거 생성

- (9) `logger = logging.getLogger(__name__)`
  - 현재 모듈 이름을 기준으로 로거를 만듭니다.
  - `logger.info(...)`, `logger.error(...)` 같은 호출로 메시지를 남깁니다.

### 4.3 14~16행: Config 클래스

- (14) `class Config:`
  - 설정 값을 한 곳에 모으는 용도.
- (15) `BASE_DIR = Path("/home/bskim/hc")`
  - 프로젝트 기준 경로.
- (16) `SQL_DIR = BASE_DIR / "sql"`
  - 실제 SQL 파일들이 들어있는 디렉터리.

> 참고: `Path`는 `/` 연산자로 경로를 결합할 수 있습니다.

### 4.4 21~24행: ListItem 레코드(NamedTuple)

- (21) `class ListItem(NamedTuple):`
  - `dataclasses` 없이도 “필드 3개짜리 레코드”를 정의.
  - 기본적으로 불변(immutable)이라 `item.sql_rel = ...` 같은 수정이 금지됩니다.
- (22) `sql_rel: str`
  - `sql/` 아래의 상대 경로(예: `sample.sql`).
- (23) `job_dt: str`
  - 날짜 같은 문자열 파라미터.
- (24) `tbl_id: str`
  - 테이블 ID 같은 문자열 파라미터.

### 4.5 29~31행: quote_bq_string

- (29) 함수 정의
- (30) docstring: “BigQuery 문자열 리터럴로 안전하게 만들기”
- (31) `return "'" + value.replace("'", "''") + "'"`
  - SQL에서 `'`는 문자열 경계이므로 내부의 `'`를 `''`로 바꿔 escape 합니다.
  - 예: `O'Reilly` → `'O''Reilly'`

### 4.6 34~43행: parse_list_line

- (34) `def parse_list_line(line: str) -> Optional[ListItem]:`
  - 한 줄을 파싱해 `ListItem`으로 만들거나, 주석/빈 줄이면 `None`.
- (35) `line = line.strip()`
  - 앞뒤 공백 제거.
- (36~37) 빈 줄 또는 `#` 주석이면 `None` 반환.
- (39) `parts = re.split(r"\s+", line)`
  - 공백이 1개 이상 연속된 경우도 처리하도록 `\s+` 사용.
- (40~41) 토큰이 3개 미만이면 입력 형식 오류이므로 `ValueError`.
- (43) 정상 케이스면 `ListItem(...)` 생성.

### 4.7 46~51행: substitute_sql

- (46) `def substitute_sql(...):`
  - SQL 템플릿 문자열에서 변수 3개를 치환.
- (47~51) `replace()` 체이닝
  - `{vs_pgm_id}` / `{vs_job_dt}` / `{vs_tbl_id}`를 각각 BigQuery 문자열로 바꿉니다.
  - 여기서 `quote_bq_string()`을 사용하므로 작은따옴표 포함 문제를 줄입니다.

### 4.8 54~60행: run_bq_query

- (54) 함수 정의
- (55~60) `subprocess.run([...], input=..., universal_newlines=True, check=True)`
  - `input=sql_text`: 표준입력으로 SQL을 넘김
  - `universal_newlines=True`: Python 3.6에서 텍스트 모드 입출력 활성화(`text=True`의 3.6 호환 대안)
  - `check=True`: 종료 코드가 0이 아니면 `CalledProcessError` 예외 발생

### 4.9 63~67행: setup_logging

- (63) 함수 정의
- (64~67) `logging.basicConfig(...)`
  - `level=logging.INFO`: INFO 이상을 출력
  - `format="%(asctime)s [%(levelname)s] %(message)s"`
    - 시간/레벨/메시지를 한 줄에 보기 좋게 출력

### 4.10 73~128행: BqJobRunner

#### 4.10.1 74~78행: 생성자
- (74) `def __init__(...)`
- (75) list 파일 경로 저장
- (76~78) 통계 카운터 초기화

#### 4.10.2 80~89행: run()
- (81~83) list 파일이 없으면 ERROR 로그 남기고 종료 코드 1
- (85~86) 파일을 읽어서 각 줄을 `process_line()`에 넘김
- (88) 마지막에 요약 출력
- (89) 실패가 하나라도 있으면 1, 모두 성공이면 0

#### 4.10.3 91~124행: process_line()
- (92) 예외 처리 시작
- (93~95) 한 줄 파싱. 주석/빈 줄이면 그냥 return
- (97~101) SQL 파일 존재 확인
  - 없으면 ERROR 로그 + fail 증가
- (103) `pgm_id = Path(item.sql_rel).stem`
  - 예: `sample.sql` → `sample`
  - `{vs_pgm_id}` 치환에 사용
- (104) SQL 파일을 문자열로 읽기
- (105~110) 템플릿 치환 적용
- (112) 실행 시작 로그(어떤 SQL을 어떤 파라미터로 돌리는지)
- (113) 실제 bq 실행
- (115~116) 성공 카운터 증가
- (118~120) 입력 형식 에러(ValueError) 처리
- (122~124) bq 실행 실패(CalledProcessError) 처리

#### 4.10.4 126~127행: print_summary()
- (127) 최종 요약을 INFO 로그로 출력

### 4.11 133~146행: main + 엔트리포인트

- (133~135) `setup_logging()`을 먼저 호출해서 로깅 포맷을 세팅
- (136~138) 인자 개수 체크
  - 사용법을 ERROR 로그로 출력
- (140~142) Runner 생성 후 실행
- (145~146) 스크립트로 직접 실행되면 `main()` 결과를 종료 코드로 반환

## 5) 학습 포인트(정리)

- `dataclasses` 없이 레코드가 필요하면 `NamedTuple`이 가장 간단한 대안
- `subprocess.run(..., check=True)` 패턴은 실패를 예외로 올려서 에러 처리 흐름이 깔끔해짐
- `logging`은 운영 환경에서 `print`보다 관리(레벨/포맷/출력 대상)가 쉬움

