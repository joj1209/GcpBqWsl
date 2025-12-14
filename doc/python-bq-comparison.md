# BigQuery Python 실행 방식 비교

이 문서는 BigQuery를 Python으로 실행하는 두 가지 방식인 **CLI 호출 방식**과 **Client Library 방식**을 비교합니다.

## 1. 방식 설명

### A. CLI 호출 방식 (`src/run_bq.py`)
Python의 `subprocess` 모듈을 사용하여 운영체제에 설치된 `bq` 명령어를 실행하는 방식입니다.

- **작동 원리**: 쉘에서 `bq query < file.sql`을 입력하는 것을 Python이 대신 수행
- **필수 조건**: Google Cloud SDK (`gcloud`, `bq`) 설치
- **인증**: `gcloud auth login` (시스템 전역 인증) 사용

### B. Client Library 방식 (`src/run_bq_lib.py`)
Google이 제공하는 공식 Python 라이브러리(`google-cloud-bigquery`)를 사용하는 방식입니다.

- **작동 원리**: Python 코드에서 BigQuery API를 직접 호출
- **필수 조건**: `pip install google-cloud-bigquery` 설치
- **인증**: Application Default Credentials (ADC) 필요 (`gcloud auth application-default login`)

## 2. 상세 비교

| 특징 | CLI 호출 방식 (Subprocess) | Client Library 방식 (API) |
| :--- | :--- | :--- |
| **실행 주체** | OS 쉘 명령어 (`bq`) | Python 코드 (API 호출) |
| **의존성** | Google Cloud SDK만 있으면 됨 | Python 패키지 설치 필요 |
| **결과 데이터** | 텍스트(String) 형태의 표 출력 | **Python 객체 (List, Dict)** |
| **데이터 가공** | 텍스트 파싱 필요 (매우 불편) | **매우 용이** (Pandas 변환 등 가능) |
| **에러 처리** | 종료 코드(Exit Code)로만 판단 | **Exception 처리**로 디테일한 제어 가능 |
| **기능 확장** | CLI 옵션(Flag)만 사용 가능 | API의 모든 세부 설정 가능 |

## 3. 추천 용도

### CLI 호출 방식이 적합한 경우
- 단순히 SQL 파일을 실행하고 결과만 눈으로 확인하면 될 때
- 별도의 라이브러리 설치가 불가능하거나 번거로운 환경일 때
- 기존 쉘 스크립트를 Python으로 단순 이관할 때

### Client Library 방식이 적합한 경우
- **데이터 분석**: 쿼리 결과를 받아 Python에서 추가 연산이나 분석을 해야 할 때
- **애플리케이션 개발**: 웹 서버나 자동화 툴 등 복잡한 로직이 필요할 때
- **안정성**: 정교한 에러 처리와 재시도 로직이 필요할 때
