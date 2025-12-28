# ADC 설정 및 run_bq_lib_json 실행 가이드

이 문서는 `py/run_bq_lib_json.py`를 **BigQuery Python Client + ADC(Application Default Credentials)** 방식으로 실행하기 위한 팀/회사용 절차를 정리합니다.

## 1) 전제

- 실행 스크립트: `py/run_bq_lib_json.py`
- SQL 파일 폴더: `sql/`
- 기준정보 CSV: `src/list/bq.csv`
- 생성되는 기준정보 JSON: `src/list/bq.json` (git 제외 권장)

## 2) Python 패키지 설치 (PEP 668 대응: venv 권장)

Debian/Ubuntu/WSL 환경에서는 PEP 668 정책으로 **시스템 Python에 `pip install`이 차단**될 수 있습니다.
따라서 프로젝트별로 **가상환경(venv)** 을 만들고 venv 내부에 패키지를 설치하는 방식을 권장합니다.

### 2.1 venv 생성/활성화

```bash
cd /home/bskim/hc
python3 -m venv .venv
source .venv/bin/activate
```

### 2.2 BigQuery Client 설치

```bash
python -m pip install -U pip
python -m pip install google-cloud-bigquery
```

설치 확인:

```bash
python -c "from google.cloud import bigquery; print('bigquery ok', getattr(bigquery, '__version__', 'unknown'))"
```

## 3) ADC(Application Default Credentials) 설정 (개발 PC/WSL용)

개발자 PC에서 가장 간단한 방법은 아래 1회 로그인입니다.

### 3.1 ADC 로그인

- 브라우저 자동 실행 가능:

```bash
gcloud auth application-default login
```

- WSL 등에서 브라우저 자동 실행이 어려움:

```bash
gcloud auth application-default login --no-launch-browser
```

명령 실행 후 출력되는 URL로 접속해 로그인/승인을 완료하고, 표시되는 verification code를 터미널에 입력합니다.

### 3.2 저장 위치

ADC는 기본적으로 아래에 저장됩니다.

- `~/.config/gcloud/application_default_credentials.json`

### 3.3 (선택) 기본 프로젝트 설정

```bash
gcloud config set project <PROJECT_ID>
```

## 4) 실행 방법

**중요:** 실행은 항상 venv 활성화 후 진행합니다.

```bash
cd /home/bskim/hc
source .venv/bin/activate
```

### 4.1 케이스별 실행

- case 1: `mid=qa`인 SQL 모두 실행

```bash
python py/run_bq_lib_json.py mid=qa
```

- case 2: 특정 SQL 1개만 실행

```bash
python py/run_bq_lib_json.py vs_pgm_id=bq_dw_red_care_sales_01.sql
```

- case 3: 특정 SQL 1개 + `vs_job_dt` 오버라이드

```bash
python py/run_bq_lib_json.py vs_pgm_id=bq_dw_red_care_sales_01.sql vs_job_dt=20251201
```

- case 4: 전체 실행 + `vs_job_dt` 오버라이드

```bash
python py/run_bq_lib_json.py vs_job_dt=20251202
```

## 5) 로그 확인

실행 로그는 날짜별로 생성됩니다.

- `log/YYYYMMDD/*.log` : INFO/WARNING
- `log/YYYYMMDD/*.log.err` : ERROR

## 6) 자주 발생하는 오류

### 6.1 `DefaultCredentialsError: Your default credentials were not found`

- 원인: ADC 미설정
- 해결:

```bash
gcloud auth application-default login
```

### 6.2 `403 Permission denied` / `Access Denied: BigQuery`

- 원인: 계정/서비스계정 권한 부족
- 해결(예시):
  - 프로젝트 단위로 `roles/bigquery.jobUser` 부여
  - 데이터셋 단위 권한(조회/쓰기) 추가 부여

## 7) 운영/배치 환경 권장(요약)

- 개발 PC: `gcloud auth application-default login` (편의)
- 운영/배치/CI: 개인 계정 대신 **서비스계정 기반** 권장
  - 가능하면 키 파일(JSON) 없이 Workload Identity Federation 또는 런타임 서비스계정 사용
