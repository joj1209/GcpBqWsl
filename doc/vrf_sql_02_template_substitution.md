# VRF SQL 생성 방식 2) 템플릿 기반 치환(string.Template)

대상 코드: app/gen_vrf_sql_template.py
템플릿 파일: app/vrf_template.sql

## 1. 개요
템플릿 기반 치환 방식은 **SQL을 외부 템플릿 파일로 분리**하고, 파이썬이 템플릿의 변수(placeholder)를 채워 넣어 SQL을 생성하는 방식입니다.

이 프로젝트에서는 `string.Template`를 사용하여 다음과 같은 placeholder를 치환합니다.
- 스칼라: `$v_stat_dt`, `$table_ref`, `$v_program_name` 등
- 블록: `$metrics_select`, `$where_clause`, `$metrics_struct`

## 2. 동작 흐름
1) `app/table.ini`를 읽어 레코드 리스트를 만든다.
2) 템플릿 파일 `app/vrf_template.sql`을 텍스트로 읽는다.
3) 각 레코드에 대해
   - 옵션 플래그에 따라 블록 문자열을 구성
   - 템플릿에 `safe_substitute()`로 치환
   - `app/out/vrf_<v_program_name>`으로 저장

## 3. 요구사항 매핑(조건부 블록)
요구사항의 “값이 없으면 pass”는 **블록 문자열을 빈 문자열로 만들어 템플릿에 넣는 방식**으로 처리합니다.

- `$metrics_select`:
  - `COUNT(DISTINCT ...)`, `SUM(...)` 라인들을 조건에 맞게 0..N줄 생성
  - 생성할 항목이 없으면 빈 문자열

- `$where_clause`:
  - `v_filter_yn=Y`면서 filter 컬럼이 있을 때만 `WHERE/AND` 라인 생성
  - 없으면 빈 문자열

- `$metrics_struct`:
  - 메트릭이 하나라도 있으면 `STRUCT(...) AS METRICS` 블록 생성
  - 없으면 빈 문자열

## 4. 장점/단점
### 장점
- **형태 유지**: SQL 포맷/주석/레이아웃을 템플릿 파일에서 관리
- **SQL 변경 비용 감소**: SQL 골격 변경은 템플릿 수정으로 해결 가능(치환 키만 유지되면)
- **역할 분리**: SQL을 데이터/문서처럼 관리 가능

### 단점
- **치환 키 의존**: 템플릿에서 `$...` 키가 바뀌면 코드도 같이 바뀌어야 함
- **빈 문자열 블록의 부작용 가능**: 템플릿이 잘못 작성되면 (예: 콤마 위치, 개행 위치) 블록이 비었을 때 구문이 깨질 수 있음
- **정확한 삽입 위치 제어가 약함**: placeholder가 “어디까지가 블록”인지 템플릿 설계에 크게 의존

## 5. 언제 이 방식을 선택하는가
- SQL 형태/주석/정렬을 템플릿에서 통일되게 유지하고 싶고
- SQL 골격이 자주 변경될 가능성이 있으며
- 치환 포인트가 비교적 단순(스칼라/블록 몇 개)할 때

## 6. 실행 방법
- `python3 app/gen_vrf_sql_template.py`

옵션:
- `--template app/vrf_template.sql`
- `--table app/table.ini`
- `--out app/out`
