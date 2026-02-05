# VRF SQL 생성 방식 1) 정적 SQL 생성(코드 조립)

대상 코드: app/gen_vrf_sql.py

## 1. 개요
정적 SQL 생성 방식은 **파이썬 코드가 SQL의 골격을 직접 문자열로 조립**하고, `app/table.ini`의 각 로우(기준정보)를 읽어 조건에 따라 일부 구문(메트릭/필터)을 추가하거나 생략하여 **완성된 SQL 파일**을 생성하는 방식입니다.

- 입력: app/table.ini
- 출력: `app/out/vrf_<v_program_name>` (로우 수만큼 파일)
- 요구사항: doc/요구사항_20260225.txt

## 2. 입력 포맷(table.ini) 처리
`app/table.ini`는 CSV 형태(첫 줄이 헤더)이며, 생성기는 `csv.DictReader`로 읽어 **헤더명을 key**로 하는 dict 레코드를 만듭니다.

주요 컬럼(요구사항 기준):
- `v_program_name`: 출력 파일명에 사용 (`vrf_` prefix)
- `v_table_name`: 테이블 참조에 사용
- `v_stat_dt`: 필터 날짜 값에 사용
- `v_use_yn`: Y가 아니면 스킵
- `v_metric_sum_yn`, `v_sum_col1`, `v_sum_col2`
- `v_metric_cnt_yn`, `v_cnt_col1`, `v_cnt_col2`
- `v_filter_yn`, `v_filter_col1`, `v_filter_col2`

## 3. 생성 규칙(요구사항 매핑)
### 3.1 파일 생성
- `app/out` 폴더를 생성(없으면)
- 기준정보 로우 수만큼 파일 생성
- 파일명: `vrf_` + `v_program_name`

### 3.2 스킵 조건
- `v_use_yn != 'Y'`이면 생성 대상에서 제외
- `v_program_name`이 비어있으면 제외

### 3.3 옵션(조건부 구문) 처리
요구사항의 “값이 없으면 pass”는 **파이썬에서 빈 문자열 체크 후 해당 구문을 아예 만들지 않음**으로 구현합니다.

- `v_metric_sum_yn=Y`:
  - `v_sum_col1`, `v_sum_col2`가 **비어있지 않은 항목만** `SUM(col)`을 생성
- `v_metric_cnt_yn=Y`:
  - `v_cnt_col1`, `v_cnt_col2`가 **비어있지 않은 항목만** `COUNT(DISTINCT col)`을 생성
- `v_filter_yn=Y`:
  - `v_filter_col1`, `v_filter_col2`가 **비어있지 않은 항목만** `WHERE/AND` 조건을 생성

## 4. 장점/단점
### 장점
- **안정성**: 템플릿 파싱 실패 같은 변수가 적고, 코드가 원하는 결과만 생성하기 쉬움
- **구문 제어가 쉬움**: 콤마/괄호/개행을 코드에서 직접 관리 가능
- **디버깅 단순**: 특정 레코드 입력 → 생성 문자열 출력의 관계가 직관적

### 단점
- **SQL 형태 유지가 약함**: 템플릿(샘플 SQL)과 동일한 스타일/주석/배치 유지가 어렵고, 변경 시 코드 수정 필요
- **SQL 변경 비용**: SQL 골격 변경 요구가 오면 파이썬 코드를 변경해야 함

## 5. 언제 이 방식을 선택하는가
- 요구사항이 “파일 생성 + 조건부 메트릭/필터”처럼 단순하고
- SQL 골격이 자주 바뀌지 않으며
- 생성 결과의 안정성이 가장 중요할 때

## 6. 실행 방법
- `python3 app/gen_vrf_sql.py`

출력:
- `app/out/vrf_*.sql`
