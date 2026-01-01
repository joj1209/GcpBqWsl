# run_bq_var_json.py 로컬 디버깅 가이드

## 현재 상황 분석

### 실행 환경
- **서버**: BQ CLI 명령어 실행 가능 (Google Cloud SDK 설치됨)
- **로컬 PC**: BQ CLI 명령어 실행 불가능
- **배포 방식**: 로컬 PC → FTP → 서버 → 실행

### 핵심 문제
`run_bq_var_json.py`는 `subprocess.run()`을 통해 `bq query` CLI 명령어를 직접 실행하므로, 로컬 PC에서는 BQ CLI가 없어 실행 불가능합니다.

```python
def run_bq_query(sql_text: str) -> None:
    """Execute BigQuery query."""
    subprocess.run(
        ["bq", "query", "--quiet", "--use_legacy_sql=false"],
        input=sql_text,
        universal_newlines=True,
        check=True,
    )
```

---

## 해결 방안

로컬 PC에서 디버깅하면서 테스트할 수 있는 **4가지 방법**을 제안합니다.

---

## 방법 1: DRY-RUN 모드 추가 (추천)

### 개념
- 실제 BQ 명령어를 실행하지 않고, 실행할 SQL과 파라미터만 출력
- 로컬에서 로직 흐름, 필터링, SQL 생성 등을 검증 가능

### 구현 방법

#### 1) 환경변수 또는 CLI 인자로 DRY-RUN 모드 활성화

```python
# Config 클래스에 추가
class Config:
    BASE_DIR = Path(__file__).resolve().parents[1]
    SQL_DIR = BASE_DIR / "sql"
    CSV_PATH = BASE_DIR / "src" / "list" / "bq.csv"
    JSON_PATH = BASE_DIR / "src" / "list" / "bq.json"
    LOG_DIR = BASE_DIR / "log"
    
    # DRY-RUN 모드 설정 (환경변수 또는 기본값)
    DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
```

#### 2) `run_bq_query()` 함수 수정

```python
def run_bq_query(sql_text: str) -> None:
    """Execute BigQuery query or print in DRY-RUN mode."""
    if Config.DRY_RUN:
        logger.info("[DRY-RUN] Would execute BQ query:")
        logger.info("-" * 60)
        logger.info(sql_text)
        logger.info("-" * 60)
        return
    
    subprocess.run(
        ["bq", "query", "--quiet", "--use_legacy_sql=false"],
        input=sql_text,
        universal_newlines=True,
        check=True,
    )
```

#### 3) 사용 방법

**로컬 PC에서 (DRY-RUN 모드)**:
```bash
# 환경변수로 설정
export DRY_RUN=true
python py/run_bq_var_json.py mid=qa

# 또는 윈도우 CMD
set DRY_RUN=true
python py/run_bq_var_json.py mid=qa

# 또는 윈도우 PowerShell
$env:DRY_RUN="true"
python py/run_bq_var_json.py mid=qa
```

**서버에서 (실제 실행)**:
```bash
# DRY_RUN 설정 안 함 (기본값 false)
python py/run_bq_var_json.py mid=qa
```

### 장점
- ✅ 코드 수정 최소화
- ✅ 로컬에서 전체 로직 흐름 검증 가능
- ✅ 생성되는 SQL 확인 가능
- ✅ 서버 배포 시 환경변수만 설정하면 됨
