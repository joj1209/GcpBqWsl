# 로컬 PC에서 run_bq_var_json.py 디버깅 하기

## 문제점
- 로컬 PC에는 BQ CLI가 없어서 run_bq_var_json.py 실행 불가
- 매번 FTP로 서버 업로드 후 테스트 → 비효율적

## 해결 방법: DRY-RUN 모드

환경변수 `DRY_RUN=true` 설정 시, 실제 BQ를 실행하지 않고 SQL만 출력

### 사용법

#### 로컬 PC (Windows CMD)
```cmd
set DRY_RUN=true
python py\run_bq_var_json.py mid=qa
```

#### 로컬 PC (Windows PowerShell)
```powershell
$env:DRY_RUN="true"
python py\run_bq_var_json.py mid=qa
```

#### 로컬 PC (Linux/Mac)
```bash
export DRY_RUN=true
python py/run_bq_var_json.py mid=qa
```

#### 서버 (실제 실행)
```bash
# DRY_RUN 없이 실행 (기본값 false)
python py/run_bq_var_json.py mid=qa
```

## 구현 방법

run_bq_var_json.py 파일에 다음 수정:

1. Config 클래스에 추가:
```python
class Config:
    # ... 기존 코드 ...
    DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
```

2. run_bq_query() 함수 수정:
```python
def run_bq_query(sql_text: str) -> None:
    if Config.DRY_RUN:
        logger.info("[DRY-RUN] Would execute BQ query:")
        logger.info(sql_text)
        return
    subprocess.run(["bq", "query", ...])  # 기존 코드
```

이제 로컬에서 전체 로직, 필터링, SQL 생성을 검증할 수 있습니다!
