-- AUTO-GENERATED
-- template: app/vrf_template_markers_v2.sql
-- source: app/table.ini
-- mid: {{mid}}
-- v_program_name: {{v_program_name}}
-- v_table_name: {{v_table_name_raw}}
-- v_stat_dt: {{v_stat_dt}}

INSERT INTO U.T (PRG_NM, TBL_NM, STAT_DT, SEQ, ALL_CNT, STATS_CNT, INS_DTM, MID, SQL_TXT)
WITH SEQ_CTE AS (
  SELECT COALESCE(MAX(SEQ), 0) + 1 AS NEXT_SEQ
    FROM U.T
   WHERE TBL_NM = "{{v_table_name_raw}}"
     AND STAT_DT = PARSE_DATE('%Y%m%d', '{{v_stat_dt}}')
),
ALL_DATA AS (
  SELECT COUNT(1) AS ALL_CNT
    FROM {{table_ref}}
),
FILTER_DATA AS (
  SELECT COUNT(1) AS FILTER_COUNT
       --__METRICS_SELECT__
    FROM {{table_ref}}
   --__WHERE_CLAUSE__
),
JSON_CTE AS (
  SELECT TO_JSON(STRUCT(
    "{{v_table_name_raw}}" AS FILTER_TYPE,
    "{{v_table_name_raw}}" AS FILTER_COLUMN,
    "{{v_stat_dt}}" AS FILTER_VALUE,
    FILTER_COUNT AS FILTER_CNT
    --__METRICS_STRUCT__
  )) AS FILTER_JSON
  FROM FILTER_DATA
)
SELECT "{{v_program_name}}" AS PRG_NM
     , "{{v_table_name_raw}}" AS TBL_NM
     , PARSE_DATE('%Y%m%d', '{{v_stat_dt}}') AS STAT_DT
     , SEQ_CTE.NEXT_SEQ AS SEQ
     , ALL_DATA.ALL_CNT AS ALL_CNT
     , JSON_CTE.FILTER_JSON AS STATS_CNT
     , CURRENT_DATETIME('Asia/Seoul') AS INS_DTM
     , "{{mid}}" AS MID
     , '''SELECT PRG_NM
     , TBL_NM
     , STAT_DT
     , SEQ
     , ALL_CNT
     , COALESCE(JSON_VALUE(STATS_CNT, '$.FILTER_COLUMN'), JSON_VALUE(STATS_CNT, '$.FILTER_TYPE')) AS FILTER_COLUMN
     , JSON_VALUE(STATS_CNT, '$.FILTER_VALUE') AS FILTER_VALUE
     , JSON_VALUE(STATS_CNT, '$.FILTER_CNT') AS FILTER_CNT
     --__METRICS_JSON_SELECT_TXT__
     , INS_DTM
     , MID
  FROM U.T
 WHERE PRG_NM = "{{v_program_name}}"
   AND TBL_NM = "{{v_table_name_raw}}"
   AND STAT_DT = PARSE_DATE('%Y%m%d', '{{v_stat_dt}}')
   AND MID = "{{mid}}"
 ORDER BY TBL_NM, SEQ
;''' AS SQL_TXT
  FROM SEQ_CTE, ALL_DATA, FILTER_DATA, JSON_CTE
;

-- Insert 결과 조회 (JSON_VALUE로 확인)
SELECT PRG_NM
     , TBL_NM
     , STAT_DT
     , SEQ
     , ALL_CNT
     , COALESCE(JSON_VALUE(STATS_CNT, '$.FILTER_COLUMN'), JSON_VALUE(STATS_CNT, '$.FILTER_TYPE')) AS FILTER_COLUMN
     , JSON_VALUE(STATS_CNT, '$.FILTER_VALUE') AS FILTER_VALUE
     , JSON_VALUE(STATS_CNT, '$.FILTER_CNT') AS FILTER_CNT
     --__METRICS_JSON_SELECT__
     , INS_DTM
     , MID
     , SQL_TXT
  FROM U.T
 WHERE PRG_NM = "{{v_program_name}}"
   AND TBL_NM = "{{v_table_name_raw}}"
   AND STAT_DT = PARSE_DATE('%Y%m%d', '{{v_stat_dt}}')
   AND MID = "{{mid}}"
 ORDER BY TBL_NM, SEQ
;
