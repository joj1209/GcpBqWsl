# VRF SQL 생성 방식 4) 컴파일된 마커 템플릿(v2 리팩토링: 단일 패스 렌더)

대상 코드: app/gen_vrf_sql_markers_v2.py  
템플릿 파일: app/vrf_template_markers_v2.sql

## 1. 목표
3번(마커 기반) 방식의 장점은 유지하면서, 다음을 개선하는 것이 목적입니다.

- 실행 효율: 파일을 여러 번 `replace()`/`splitlines()` 하며 재탐색하지 않기
- 안정성: 인라인 마커를 없애고(취약), 라인 마커만 사용하여 조작 범위를 더 명확히
- 유지보수: 템플릿이 출력 포맷(들여쓰기)을 주도하고, 코드는 “무엇을 넣을지”만 결정

## 2. 핵심 아이디어
### 2.1 템플릿을 1회 컴파일
`CompiledMarkerTemplate`는 템플릿을 읽어
- 각 마커가 정확히 1개인지 검증
- 마커가 위치한 라인의 들여쓰기(indent)를 기억

이후 각 레코드 렌더링 시에는 **한 번의 루프**로:
- 스칼라 placeholder(`{{name}}`) 치환
- 마커 라인 치환(삽입할 블록이 없으면 라인 자체 제거)

## 3. 템플릿 포맷이 출력 포맷을 결정
`app/vrf_template_markers_v2.sql`에서는 마커 라인의 들여쓰기를 의도적으로 배치합니다.

예:
- `       --__METRICS_SELECT__`  (메트릭 SELECT 라인의 기본 들여쓰기)
- `   --__WHERE_CLAUSE__`        (WHERE/AND의 기본 들여쓰기)
- `    --__METRICS_STRUCT__`     (STRUCT(...) 블록이 들어갈 기본 들여쓰기)

코드는 삽입할 라인 내용을 `", COUNT(...)"`처럼 **좌측 공백 없이** 만들고,
실제 들여쓰기는 템플릿이 지정한 indent를 적용합니다.

## 4. 마커 종류(모두 라인 마커)
- `--__METRICS_SELECT__`
- `--__WHERE_CLAUSE__`
- `--__METRICS_STRUCT__`

모두 “해당 라인 전체”가 교체되므로, 치환 범위가 명확하고 템플릿이 흔들려도 실패가 빠릅니다.

## 5. 요구사항 매핑
요구사항(20260225)의 “Y일 때만 구현, 값 없으면 pass”는 블록 라인 생성 단계에서 해결합니다.

- 메트릭: 플래그가 Y이고 컬럼 값이 있을 때만 해당 라인을 리스트에 추가
- 필터: 플래그가 Y이고 컬럼 값이 있을 때만 WHERE/AND 라인을 리스트에 추가
- 메트릭 struct: 메트릭이 1개 이상 있을 때만 블록 생성

## 6. 실행 방법
- `python3 app/gen_vrf_sql_markers_v2.py`

옵션:
- `--template app/vrf_template_markers_v2.sql`
- `--table app/table.ini`
- `--out app/out`
