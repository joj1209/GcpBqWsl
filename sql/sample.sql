-- 1. 스크립팅 시작을 알리는 BEGIN
BEGIN
    -- ===================================
    -- 1. 변수 선언 (DECLARE)
    -- ===================================
    DECLARE vs_target_date DATE DEFAULT DATE('2025-12-10');
    DECLARE vs_min_amount NUMERIC DEFAULT 1000.00;
    DECLARE vs_record_count INT64;

    -- ===================================
    -- 2. 변수에 값 할당 (SET 또는 SELECT INTO)
    -- ===================================
    
    -- SET 문을 사용하여 변수에 직접 값을 할당
    SET vs_target_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    -- SELECT INTO를 사용하여 쿼리 결과를 변수에 할당
    -- (실제 테이블 이름으로 변경 필요)
    SELECT
        COUNT(1) INTO vs_record_count
    FROM
        `your_dataset.your_sales_table`
    WHERE
        sale_date = vs_target_date;

    -- ===================================
    -- 3. 조건문 (IF...THEN...ELSE)
    -- ===================================
    IF vs_record_count > 0 THEN
        -- 조건이 참일 경우 실행할 SQL 문
        SELECT
            '✅ 판매 데이터가 존재합니다. 최소 금액 이상의 거래를 조회합니다.' AS status_message,
            vs_record_count AS total_records;

        -- 변수 값을 활용하여 쿼리 실행
        SELECT
            transaction_id,
            sale_amount
        FROM
            `your_dataset.your_sales_table`
        WHERE
            sale_date = vs_target_date AND sale_amount >= vs_min_amount
        LIMIT 5;

    ELSE
        -- 조건이 거짓일 경우 실행할 SQL 문
        SELECT
            '❌ 지정된 날짜에 판매 데이터가 없습니다.' AS status_message,
            vs_target_date AS checked_date;
            
    END IF;

    -- ===================================
    -- 4. 마지막 결과 반환
    -- ===================================
    -- 스크립트의 최종 결과를 출력합니다.
    SELECT CURRENT_TIMESTAMP() AS script_end_time;

END; -- 스크립팅 종료