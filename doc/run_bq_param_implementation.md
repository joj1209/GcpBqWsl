# run_bq_param.py 구현 문서

## 개요

`run_bq_param.py`는 BigQuery 파라미터화된 쿼리를 실행하는 프로그램입니다. `run_bq_lib_json.py`와 유사한 구조를 가지지만, SQL 문자열 치환 대신 BigQuery의 네이티브 파라미터 기능을 사용합니다.

**작성일**: 2026-01-07  
**버전**: 1.0.0  
**기반 프로그램**: `run_bq_lib_json.py`

---

## 주요 특징

### 1. 파라미터 전달 방식

#### 기존 방식 (run_bq_var_json.py, run_bq_lib_json.py)
```python
# SQL 파일에서 문자열 치환
template = "SELECT '{vs_pgm_id}', '{vs_job_dt}', '{vs_tbl_id}'"
sql_text = template.replace("{vs_pgm_id}", quote_bq_string(pgm_id))
```

#### 신규 방식 (run_bq_param.py)
```python
# BigQuery 파라미터로 전달
sql_text = "SELECT @program_id, @standard_date, @target_table"
cmd = [
    "bq", "query",
    "--parameter=program_id:STRING:value1",
    "--parameter=standard_date:STRING:value2",
    ...
]
```

### 2. 실행 방식

- **라이브러리**: 사용하지 않음 (google-cloud-bigquery 불필요)
- **CLI 도구**: `bq` 명령어 사용
- **프로세스**: `subprocess.run()` 호출

### 3. SQL 디렉토리

- **대상 폴더**: `sql_param/`
- **SQL 형식**: 파라미터에 `@` 접두사 사용

---

## 파라미터 매핑

### CSV/JSON 컬럼 → SQL 파라미터

| CSV 컬럼명 | SQL 파라미터명 | 타입 | 기본값 | 설명 |
|-----------|---------------|------|--------|------|
| `vs_pgm_id` | `@program_id` | STRING | (파일명) | SQL 파일명 (확장자 제외) |
| `vs_job_dt` | `@standard_date` | STRING | - | 기준 일자 (YYYYMMDD) |
| `vs_tbl_id` | `@target_table` | STRING | - | 대상 테이블명 |
| `job_seq` | `@job_seq` | STRING | "1" | 작업 차수 |
| `temp_table` | `@temp_table` | STRING | "" | 임시 테이블명 |

---

## 사용 방법

### Case 1: mid 필터
```bash
python py/run_bq_param.py mid=qa
```

### Case 2: 특정 SQL 파일 실행
```bash
python py/run_bq_param.py vs_pgm_id=bq_dw_red_care_sales_01.sql
```

### Case 3: 파라미터 오버라이드
```bash
python py/run_bq_param.py vs_pgm_id=sample.sql vs_job_dt=20251201
```

### Case 4: 전체 실행 + 오버라이드
```bash
python py/run_bq_param.py vs_job_dt=20251202 job_seq=2
```

---

## run_bq_lib_json.py와의 차이점

| 항목 | run_bq_lib_json.py | run_bq_param.py |
|------|-------------------|----------------|
| **SQL 디렉토리** | `sql/` | `sql_param/` |
| **실행 방식** | Python 라이브러리 | subprocess + bq CLI |
| **변수 치환** | 문자열 replace | BigQuery 파라미터 |
| **SQL 형식** | `{vs_pgm_id}` | `@program_id` |
| **의존성** | google-cloud-bigquery | bq CLI만 필요 |
| **파라미터 개수** | 3개 | 5개 |

---

## 커밋 정보

**커밋 메시지**: feat: Add run_bq_param.py with subprocess-based parameterized query execution

**변경사항**:
- py/run_bq_param.py 신규 생성 (301줄)
- doc/요구사항.txt 업데이트
- subprocess 기반 파라미터화된 쿼리 실행 구현
- 5개 파라미터 지원 (@program_id, @standard_date, @target_table, @job_seq, @temp_table)
- sql_param 폴더의 SQL 파일 처리

**푸시 결과**:
- 커밋 ID: 086bc36
- 변경 파일: 2개
- 추가: 231줄
- 삭제: 106줄

---

**문서 작성일**: 2026-01-07
