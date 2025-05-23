CREATE OR REPLACE PROCEDURE `my-project-demo-dwh.dwh_silver.load_silver` (IN execution_date DATE)

BEGIN
    DECLARE start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE end_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE batch_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE batch_end_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE rows_affected INT64;
    DECLARE error_message STRING;
    DECLARE error_stack STRING;
    DECLARE step_name STRING;

    CREATE TABLE IF NOT EXISTS `my-project-demo-dwh.dwh_silver.procedure_logs` (
        log_id STRING,
        execution_ts TIMESTAMP,
        procedure_name STRING,
        step_name STRING,
        status STRING,
        rows_affected INT64,
        message STRING,
        error STRING,
        stack_trace STRING
    )
    OPTIONS(description='Procedure execution logs');

    -- Initialize procedure
    SET batch_start_time = CURRENT_TIMESTAMP();

    -- Log procedure start
    INSERT INTO `my-project-demo-dwh.dwh_silver.procedure_logs`
    VALUES (
            GENERATE_UUID(),
            batch_start_time,
            'load_data_into_silver_dataset',
            'PROCEDURE_START',
            'RUNNING',
            NULL,
            CONCAT('Starting procedure execution for date: ', CAST(execution_date AS STRING)),
            NULL,
            NULL
    );

    BEGIN
        -- Step 1: Load raw data into staging
        SET step_name = 'load_silver_crm_cust_info';

        -- Loading silver.crm_cust_info
        SET start_time = CURRENT_TIMESTAMP();

        TRUNCATE TABLE `my-project-demo-dwh.dwh_silver.crm_cust_info`;

        INSERT INTO `my-project-demo-dwh.dwh_silver.crm_cust_info`
        (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )

        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                ELSE 'n/a'
            END,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date

        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM `my-project-demo-dwh.dwh_bronze.crm_cust_info`
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1; -- Select the most recent record per customer

        SET end_time = CURRENT_TIMESTAMP();

        SET rows_affected = @@row_count;

        -- Log successful step completion
        INSERT INTO `my-project-demo-dwh.dwh_silver.procedure_logs`
        VALUES (
                GENERATE_UUID(),
                CURRENT_TIMESTAMP(),
                'load_data_into_silver_dataset',
                step_name,
                'COMPLETED',
                rows_affected,
                CONCAT('Successfully loaded ', CAST(rows_affected AS STRING), ' records in ', CAST(TIMESTAMP_DIFF(end_time, start_time, SECOND) AS STRING), ' seconds'),
                NULL,
                NULL
        );

        -- Loading silver.crm_prd_info
        SET step_name = 'load_silver_crm_prd_info';

        SET start_time = CURRENT_TIMESTAMP();

        TRUNCATE TABLE `my-project-demo-dwh.dwh_silver.crm_prd_info`;

        INSERT INTO `my-project-demo-dwh.dwh_silver.crm_prd_info`
        (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )

        SELECT 
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
            SUBSTRING(prd_key, 7),
            prd_nm,
            COALESCE(CAST(prd_cost as int64), 0),
            CASE 
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Roads'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                ELSE 'n/a'
            END AS prd_line,
            prd_start_dt,
            LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_end_dt) - 1 -- Calculate end date as one day before the next start date

        FROM `my-project-demo-dwh.dwh_bronze.crm_prd_info`;

        SET end_time = CURRENT_TIMESTAMP();

        SET rows_affected = @@row_count;

        -- Log successful step completion
        INSERT INTO `my-project-demo-dwh.dwh_silver.procedure_logs`
        VALUES (
                GENERATE_UUID(),
                CURRENT_TIMESTAMP(),
                'load_data_into_silver_dataset',
                step_name,
                'COMPLETED',
                rows_affected,
                CONCAT('Successfully loaded ', CAST(rows_affected AS STRING), ' records in ', CAST(TIMESTAMP_DIFF(end_time, start_time, SECOND) AS STRING), ' seconds'),
                NULL,
                NULL
        );

        -- Loading crm_sales_details
        SET step_name = 'load_silver_crm_sales_details';

        SET start_time = CURRENT_TIMESTAMP();

        TRUNCATE TABLE `my-project-demo-dwh.dwh_silver.crm_sales_details`;

        INSERT INTO `my-project-demo-dwh.dwh_silver.crm_sales_details`(
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )

        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt <= 0 OR LENGTH(cast(sls_order_dt as STRING)) != 8 THEN NULL
                ELSE PARSE_DATE('%Y%m%d', cast(sls_order_dt as STRING)) END,
            CASE WHEN sls_ship_dt <= 0 OR LENGTH(CAST(sls_ship_dt as STRING)) != 8 THEN NULL
                ELSE PARSE_DATE('%Y%m%d', cast(sls_ship_dt as STRING)) END,
            CASE WHEN sls_due_dt <= 0 OR LENGTH(cast(sls_due_dt as STRING)) != 8 THEN NULL
                ELSE PARSE_DATE('%Y%m%d', cast(sls_due_dt as STRING)) END,
            CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_price * sls_quantity)
                THEN ABS(sls_price * sls_quantity)
                ELSE sls_sales END,
            sls_quantity,
            CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price

        FROM `my-project-demo-dwh.dwh_bronze.crm_sales_details`;

        SET end_time = CURRENT_TIMESTAMP();

        SET rows_affected = @@row_count;

        -- Log successful step completion
        INSERT INTO `my-project-demo-dwh.dwh_silver.procedure_logs`
        VALUES (
                GENERATE_UUID(),
                CURRENT_TIMESTAMP(),
                'load_data_into_silver_dataset',
                step_name,
                'COMPLETED',
                rows_affected,
                CONCAT('Successfully loaded ', CAST(rows_affected AS STRING), ' records in ', CAST(TIMESTAMP_DIFF(end_time, start_time, SECOND) AS STRING), ' seconds'),
                NULL,
                NULL
        );

        -- Loading erp_cust_az12
        SET step_name = 'load_silver_erp_cust_az12';

        SET start_time = CURRENT_TIMESTAMP();

        TRUNCATE TABLE `my-project-demo-dwh.dwh_silver.erp_cust_az12`;

        INSERT INTO `my-project-demo-dwh.dwh_silver.erp_cust_az12`
        (
            cid,
            bdate,
            gen
        )

        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END,
            CASE WHEN bdate > CURRENT_DATE THEN NULL ELSE bdate END,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END -- Normalize gender values and handle unknown cases

        FROM `my-project-demo-dwh.dwh_silver.erp_cust_az12`;

        SET end_time = CURRENT_TIMESTAMP();

        SET rows_affected = @@row_count;

        -- Log successful step completion
        INSERT INTO `my-project-demo-dwh.dwh_silver.procedure_logs`
        VALUES (
                GENERATE_UUID(),
                CURRENT_TIMESTAMP(),
                'load_data_into_silver_dataset',
                step_name,
                'COMPLETED',
                rows_affected,
                CONCAT('Successfully loaded ', CAST(rows_affected AS STRING), ' records in ', CAST(TIMESTAMP_DIFF(end_time, start_time, SECOND) AS STRING), ' seconds'),
                NULL,
                NULL
        );

        -- Loading erp_loc_a101
        SET step_name = 'load_silver_erp_loc_a101';

        SET start_time = CURRENT_TIMESTAMP();

        TRUNCATE TABLE `my-project-demo-dwh.dwh_silver.erp_loc_a101`;

        INSERT INTO `my-project-demo-dwh.dwh_silver.erp_loc_a101`
        (
            cid,
            cntry	
        )

        SELECT 
            REPLACE(cid, '-', ''),
            CASE 
                WHEN UPPER(TRIM(cntry)) IN ('US','USA','UNITED STATES','UNITED STATES OF AMERICA') THEN 'United States'
                WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END -- Normalize and Handle missing or blank country codes

        FROM `my-project-demo-dwh.dwh_silver.erp_loc_a101`;

        SET end_time = CURRENT_TIMESTAMP();

        SET rows_affected = @@row_count;

        -- Log successful step completion
        INSERT INTO `my-project-demo-dwh.dwh_silver.procedure_logs`
        VALUES (
                GENERATE_UUID(),
                CURRENT_TIMESTAMP(),
                'load_data_into_silver_dataset',
                step_name,
                'COMPLETED',
                rows_affected,
                CONCAT('Successfully loaded ', CAST(rows_affected AS STRING), ' records in ', CAST(TIMESTAMP_DIFF(end_time, start_time, SECOND) AS STRING), ' seconds'),
                NULL,
                NULL
        );

        -- Loading erp_px_cat_g1v2 
        SET step_name = 'load_silver_erp_px_cat_g1v2';

        SET start_time = CURRENT_TIMESTAMP();

        TRUNCATE TABLE `my-project-demo-dwh.dwh_silver.erp_px_cat_g1v2`;

        INSERT INTO `my-project-demo-dwh.dwh_silver.erp_px_cat_g1v2`
        (
            id,
            cat,
            subcat,
            maintenance
        )

        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM `my-project-demo-dwh.dwh_bronze.erp_px_cat_g1v2`;

        SET end_time = CURRENT_TIMESTAMP();

        SET rows_affected = @@row_count;

        -- Log successful step completion
        INSERT INTO `my-project-demo-dwh.dwh_silver.procedure_logs`
        VALUES (
                GENERATE_UUID(),
                CURRENT_TIMESTAMP(),
                'load_data_into_silver_dataset',
                step_name,
                'COMPLETED',
                rows_affected,
                CONCAT('Successfully loaded ', CAST(rows_affected AS STRING), ' records in ', CAST(TIMESTAMP_DIFF(end_time, start_time, SECOND) AS STRING), ' seconds'),
                NULL,
                NULL
        );

        SET batch_end_time = CURRENT_TIMESTAMP();

        -- Log successful procedure completion
        INSERT INTO `my-project-demo-dwh.dwh_silver.procedure_logs`
        VALUES (
                GENERATE_UUID(),
                CURRENT_TIMESTAMP(),
                'load_data_into_silver_dataset',
                'PROCEDURE_END',
                'COMPLETED',
                NULL,
                CONCAT('Procedure completed successfully in ', 
                        CAST(TIMESTAMP_DIFF(batch_end_time, batch_start_time, SECOND) AS STRING), ' seconds'),
                NULL,
                NULL
        );

    EXCEPTION WHEN ERROR THEN
        -- Capture error details with proper formatting
        SET error_message = @@error.message;
        SET error_stack = FORMAT('%T', @@error.stack_trace);
        -- Log error
        INSERT INTO `my-project-demo-dwh.dwh_silver.procedure_logs`
        VALUES(
                GENERATE_UUID(),
                CURRENT_TIMESTAMP(),
                'load_data_into_silver_dataset',
                COALESCE(step_name, 'UNKNOWN_STEP'),
                'FAILED',
                NULL,
                CONCAT('Error during step: ', COALESCE(step_name, 'UNKNOWN_STEP')),
                error_message,
                error_stack
        );
    
        -- Re-throw the error to ensure Airflow task fails
        RAISE USING MESSAGE = error_message;
    END;
END;