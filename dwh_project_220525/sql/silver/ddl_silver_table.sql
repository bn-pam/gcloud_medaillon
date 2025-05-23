CREATE OR REPLACE TABLE `my-project-demo-dwh.dwh_silver.crm_cust_info` (
    cst_id INT64,
    cst_key STRING,
    cst_firstname STRING,
    cst_lastname STRING,
    cst_marital_status STRING,
    cst_gndr STRING,
    cst_create_date DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP() -- Metadata columns added
);

CREATE OR REPLACE TABLE `my-project-demo-dwh.dwh_silver.crm_prd_info` (
    prd_id INT64,
    cat_id STRING,
    prd_key STRING,
    prd_nm STRING,
    prd_cost INT64,
    prd_line STRING,
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP() -- Metadata columns added
);

CREATE OR REPLACE TABLE `my-project-demo-dwh.dwh_silver.crm_sales_details` (
    sls_ord_num STRING,
    sls_prd_key STRING,
    sls_cust_id INT64,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT64,
    sls_quantity INT64,
    sls_price FLOAT64,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP() -- Metadata columns added
);

CREATE OR REPLACE TABLE `my-project-demo-dwh.dwh_silver.erp_loc_a101` (
    cid STRING,
    cntry STRING,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP() -- Metadata columns added
);

CREATE OR REPLACE TABLE `my-project-demo-dwh.dwh_silver.erp_cust_az12` (
    cid STRING,
    bdate DATE,
    gen STRING,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP() -- Metadata columns added
);

CREATE OR REPLACE TABLE `my-project-demo-dwh.dwh_silver.erp_px_cat_g1v2` (
    id STRING,
    cat STRING,
    subcat STRING,
    maintenance BOOL,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP() -- Metadata columns added
);