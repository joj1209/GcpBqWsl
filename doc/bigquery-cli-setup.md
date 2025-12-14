# WSL에서 Google BigQuery CLI 환경설정 가이드

## 설치 완료 ✓

### 1. Google Cloud SDK 설치
```bash
curl https://sdk.cloud.google.com | bash
```

### 2. 환경변수 설정
셸을 재시작하거나 다음 명령어로 PATH를 활성화:
```bash
source ~/hc/google-cloud-sdk/path.bash.inc
source ~/hc/google-cloud-sdk/completion.bash.inc
```

또는 `~/.bashrc`에 영구적으로 추가:
```bash
echo 'source ~/hc/google-cloud-sdk/path.bash.inc' >> ~/.bashrc
echo 'source ~/hc/google-cloud-sdk/completion.bash.inc' >> ~/.bashrc
```

### 3. Google Cloud 인증
```bash
gcloud init --console-only
```

브라우저에서 제공된 URL로 이동하여 로그인 후, 인증 코드를 터미널에 입력합니다.

### 4. 설정 확인
```bash
# gcloud 버전 확인
gcloud --version

# BigQuery CLI 버전 확인
bq version

# BigQuery 데이터셋 목록 조회
bq ls
```

## BigQuery CLI 주요 명령어

### 데이터셋 조회
```bash
bq ls
```

### 테이블 조회
```bash
bq ls [DATASET_ID]
```

### 쿼리 실행
```bash
bq query --use_legacy_sql=false 'SELECT * FROM \`project.dataset.table\` LIMIT 10'
```

### 테이블 정보 조회
```bash
bq show [DATASET_ID].[TABLE_ID]
```

### 데이터셋 생성
```bash
bq mk [DATASET_ID]
```

### 테이블 생성
```bash
bq mk -t [DATASET_ID].[TABLE_ID] [SCHEMA]
```

## 현재 설정 정보

- **Google Cloud SDK**: 549.0.1
- **BigQuery CLI**: 2.1.25
- **인증 계정**: joj1209@gmail.com
- **프로젝트**: project-5ae95497-348c-4e84-aed
- **데이터셋**: DM, DW

## 참고 자료
- [BigQuery CLI 공식 문서](https://cloud.google.com/bigquery/docs/bq-command-line-tool)
- [gcloud CLI 문서](https://cloud.google.com/sdk/gcloud)
