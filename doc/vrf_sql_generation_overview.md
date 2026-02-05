# VRF SQL 생성 방식 개요(1~4)

이 문서는 20260225 요구사항( `app/table.ini` 기반으로 SQL 파일 생성 )을 구현한 4가지 방식의 차이/선택 기준을 한 눈에 보기 위한 인덱스입니다.

## 빠른 링크
- 1) 정적 생성: [doc/vrf_sql_01_static_generation.md](doc/vrf_sql_01_static_generation.md)
- 2) 템플릿 치환: [doc/vrf_sql_02_template_substitution.md](doc/vrf_sql_02_template_substitution.md)
- 3) 마커 기반: [doc/vrf_sql_03_marker_based_substitution.md](doc/vrf_sql_03_marker_based_substitution.md)
- 4) 마커 기반(v2 리팩토링/컴파일): [doc/vrf_sql_04_marker_compiled_refactor.md](doc/vrf_sql_04_marker_compiled_refactor.md)

## 실행 방법(공통)
입력:
- `app/table.ini` (CSV 헤더 기반)

출력:
- `app/out/vrf_<v_program_name>`

실행:
- 1) `python3 app/gen_vrf_sql.py`
- 2) `python3 app/gen_vrf_sql_template.py`
- 3) `python3 app/gen_vrf_sql_markers.py`
- 4) `python3 app/gen_vrf_sql_markers_v2.py`

## 1~4 비교표(선택 가이드)

| 번호 | 방식 | 소스 파일 | 템플릿 파일 | SQL “형태” 소유 | 옵션 블록 처리 | 가드레일(템플릿 변경 탐지) | 추천 상황 |
|---:|---|---|---|---|---|---|---|
| 1 | 정적 생성(코드 조립) | app/gen_vrf_sql.py | 없음 | 코드 | 코드에서 if로 조립 | 낮음(코드만 신뢰) | 템플릿 관리가 불필요, SQL 구조가 자주 바뀌지 않음 |
| 2 | 템플릿 치환(플레이스홀더) | app/gen_vrf_sql_template.py | app/vrf_template.sql | 템플릿(플레이스홀더) | placeholder에 문자열 삽입 | 중간(placeholder 누락/오타는 조용히 실패 가능) | SQL 형태는 템플릿이 주도, 치환 포인트가 단순 |
| 3 | 마커 기반(명시적 삽입 포인트) | app/gen_vrf_sql_markers.py | app/vrf_template_markers.sql | 템플릿(마커) | 마커 위치에 블록 삽입/제거 | 높음(마커 1회 존재 강제 등) | 템플릿 형태 보존 + 삽입/삭제 위치를 강하게 통제 |
| 4 | 마커 기반 v2(컴파일+단일 패스) | app/gen_vrf_sql_markers_v2.py | app/vrf_template_markers_v2.sql | 템플릿(마커+indent) | 마커 라인 전체 교체(없으면 제거) | 높음(마커 1회 강제 + 미치환 {{name}} 탐지) | 3번을 운영용으로 다듬고 싶음(안정/성능/포맷 일관성) |

## 4번(v2)에서 추가로 좋아진 점 요약
- “템플릿 컴파일 1회 + 레코드 렌더 1회 루프”로 불필요한 재탐색/다중 replace 감소
- 인라인 마커를 제거하고 “라인 마커”만 사용해서 치환 범위가 더 명확
- 마커 라인의 들여쓰기를 기준으로 삽입 라인의 들여쓰기를 자동 적용(템플릿이 포맷을 소유)
- `{{name}}` placeholder가 남아있으면 즉시 실패하도록 검증(조용한 오류 방지)

## 실무 추천(간단)
- 템플릿을 팀이 같이 관리하고, 포맷/구조 안정성이 중요하면: 4번
- 템플릿은 쓰되 단순 치환만 필요하면: 2번
- 코드에서 모든 걸 통제하고 템플릿 파일을 없애고 싶으면: 1번
- 3번은 4번으로 자연스럽게 대체 가능(기존 템플릿/마커 규약을 유지해야 하면 3번 유지)
