-- 1. 스크립팅 시작을 알리는 BEGIN
BEGIN
    -- ===================================
    -- 1. 변수 선언 (DECLARE)
    -- ===================================
    DECLARE vs_target_date DATE DEFAULT DATE('2025-12-10');
    DECLARE vs_min_amount NUMERIC DEFAULT 1000.00;
    DECLARE vs_record_count INT64;

    -- ===================================
    -- 2. 10개의 블록 실행
    -- ===================================

    -- Block 1
    BEGIN
        SELECT 1 AS block_number, 'Start' AS status;
        SET vs_target_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
        SET vs_record_count = (SELECT COUNT(1) FROM DW.RED_CARE_SALES WHERE sale_date = vs_target_date);
        SELECT 1 AS block_number, vs_record_count as record_count, CURRENT_TIMESTAMP() AS script_end_time;
    END;

    -- Block 2
    BEGIN
        SELECT 2 AS block_number, 'Start' AS status;
        SET vs_target_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
        SET vs_record_count = (SELECT COUNT(1) FROM DW.RED_CARE_SALES WHERE sale_date = vs_target_date);
        SELECT 2 AS block_number, vs_record_count as record_count, CURRENT_TIMESTAMP() AS script_end_time;
    END;

    -- Block 3
    BEGIN
        SELECT 3 AS block_number, 'Start' AS status;
        SET vs_target_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
        SET vs_record_count = (SELECT COUNT(1) FROM DW.RED_CARE_SALES WHERE sale_date = vs_target_date);
        SELECT 3 AS block_number, vs_record_count as record_count, CURRENT_TIMESTAMP() AS script_end_time;
    END;

    -- Block 4
    BEGIN
        SELECT 4 AS block_number, 'Start' AS status;
        SET vs_target_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
        SET vs_record_count = (SELECT COUNT(1) FROM DW.RED_CARE_SALES WHERE sale_date = vs_target_date);
        SELECT 4 AS block_number, vs_record_count as record_count, CURRENT_TIMESTAMP() AS script_end_time;
    END;

    -- Block 5
    BEGIN
        SELECT 5 AS block_number, 'Start' AS status;
        SET vs_target_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
        SET vs_record_count = (SELECT COUNT(1) FROM DW.RED_CARE_SALES WHERE sale_date = vs_target_date);
        SELECT 5 AS block_number, vs_record_count as record_count, CURRENT_TIMESTAMP() AS script_end_time;
    END;

    -- Block 6
    BEGIN
        SELECT 6 AS block_number, 'Start' AS status;
        SET vs_target_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
        SET vs_record_count = (SELECT COUNT(1) FROM DW.RED_CARE_SALES WHERE sale_date = vs_target_date);
        SELECT 6 AS block_number, vs_record_count as record_count, CURRENT_TIMESTAMP() AS script_end_time;
    END;

    -- Block 7
    BEGIN
        SELECT 7 AS block_number, 'Start' AS status;
        SET vs_target_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
        SET vs_record_count = (SELECT COUNT(1) FROM DW.RED_CARE_SALES WHERE sale_date = vs_target_date);
        SELECT 7 AS block_number, vs_record_count as record_count, CURRENT_TIMESTAMP() AS script_end_time;
    END;

    -- Block 8
    BEGIN
        SELECT 8 AS block_number, 'Start' AS status;
        SET vs_target_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
        SET vs_record_count = (SELECT COUNT(1) FROM DW.RED_CARE_SALES WHERE sale_date = vs_target_date);
        SELECT 8 AS block_number, vs_record_count as record_count, CURRENT_TIMESTAMP() AS script_end_time;
    END;

    -- Block 9
    BEGIN
        SELECT 9 AS block_number, 'Start' AS status;
        SET vs_target_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
        SET vs_record_count = (SELECT COUNT(1) FROM DW.RED_CARE_SALES WHERE sale_date = vs_target_date);
        SELECT 9 AS block_number, vs_record_count as record_count, CURRENT_TIMESTAMP() AS script_end_time;
    END;

    -- Block 10
    BEGIN
        SELECT 10 AS block_number, 'Start' AS status;
        SET vs_target_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
        SET vs_record_count = (SELECT COUNT(1) FROM DW.RED_CARE_SALES WHERE sale_date = vs_target_date);
        SELECT 10 AS block_number, vs_record_count as record_count, CURRENT_TIMESTAMP() AS script_end_time;
    END;

END; -- 스크립팅 종료
