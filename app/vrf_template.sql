-- AUTO-GENERATED
-- template: app/vrf_template.sql
-- source: app/table.ini
-- mid: $mid
-- v_program_name: $v_program_name
-- v_table_name: $v_table_name_raw
-- v_stat_dt: $v_stat_dt

INSERT INTO U.T
WITH SEQ_CTE AS (
  SELECT COALESCE(MAX(SEQ), 0) + 1 AS NEXT_SEQ
    FROM U.T
   WHERE TBL_NM = "$v_table_name_raw"
     AND STAT_DT = PARSE_DATE('%Y%m%d', '$v_stat_dt')
),
ALL_DATA AS (
  SELECT COUNT(1) AS ALL_CNT
    FROM $table_ref
),
FILTER_DATA AS (
  SELECT COUNT(1) AS FILTER_COUNT
$metrics_select
    FROM $table_ref
$where_clause
),
JSON_CTE AS (
  SELECT TO_JSON(STRUCT(
    "$v_table_name_raw" AS FILTER_TYPE,
    "$v_stat_dt" AS FILTER_VALUE,
    FILTER_COUNT AS FILTER_CNT$metrics_struct
  )) AS FILTER_JSON
  FROM FILTER_DATA
)
SELECT "$v_program_name" AS PRG_NM
     , "$v_table_name_raw" AS TBL_NM
     , PARSE_DATE('%Y%m%d', '$v_stat_dt') AS STAT_DT
     , SEQ_CTE.NEXT_SEQ AS SEQ
     , ALL_DATA.ALL_CNT AS ALL_CNT
     , JSON_CTE.FILTER_JSON AS STATS_CNT
     , CURRENT_DATETIME('Asia/Seoul') AS INS_DTM
  FROM SEQ_CTE, ALL_DATA, FILTER_DATA, JSON_CTE
;
