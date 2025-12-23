-- 1. 스크립팅 시작을 알리는 BEGIN
BEGIN
/*********************************************************/
/* PGM ID : bq_dw_red_care_sales_04.sql                        */
/* TBL_ID : DW.RED_CARE_SALES                              */
/*********************************************************/
    -- ===================================
    -- 1. 변수 선언 (DECLARE)
    -- ===================================
    DECLARE vs_pgm_id STRING DEFAULT {vs_pgm_id};
    DECLARE vs_tbl_id STRING DEFAULT {vs_tbl_id};
    DECLARE vs_job_dt STRING DEFAULT {vs_job_dt};
    DECLARE vs_target_date DATE DEFAULT DATE('2025-12-10');
    DECLARE vs_min_amount NUMERIC DEFAULT 1000.00;
    DECLARE vs_record_count INT64;

    -- ===================================
    -- 2. 변수에 값 할당 (SET 또는 SELECT INTO)
    -- ===================================
    BEGIN
        -- SET 문을 사용하여 변수에 직접 값을 할당
        SET vs_target_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

        -- SELECT 결과를 변수에 할당 (SET 사용)
        -- (실제 테이블 이름으로 변경 필요)
        SET vs_record_count = (
            SELECT
                COUNT(1)
            FROM
                DW.RED_CARE_SALES
            WHERE
                sale_date = vs_target_date
        );

        select vs_record_count as record_count;

        -- ===================================
        -- 4. 마지막 결과 반환
        -- ===================================
        -- 스크립트의 최종 결과를 출력합니다.
        SELECT CURRENT_TIMESTAMP() AS script_end_time;
    END;

    BEGIN
        -- SET 문을 사용하여 변수에 직접 값을 할당
        SET vs_target_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

        -- SELECT 결과를 변수에 할당 (SET 사용)
        -- (실제 테이블 이름으로 변경 필요)
        SET vs_record_count = (
            SELECT
                COUNT(1)
            FROM
                DW.RED_CARE_SALES
            WHERE
                sale_date = vs_target_date
        );

        select vs_record_count as record_count;

        -- ===================================
        -- 4. 마지막 결과 반환
        -- ===================================
        -- 스크립트의 최종 결과를 출력합니다.
        SELECT CURRENT_TIMESTAMP() AS script_end_time;
    END;

END; -- 스크립팅 종료
