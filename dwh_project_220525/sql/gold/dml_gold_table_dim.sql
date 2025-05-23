-- =============================================================================
-- Création Table de Dimension: gold.dim_customers
-- =============================================================================

-- Stores customer details enriched with demographic and geographic data.

CREATE OR REPLACE VIEW `my-project-demo-dwh.dwh_gold.dim_customers` AS
select 
	row_number() over(order by cci.cst_id) as customer_key, --Generated Surroagate key to use as primary key
	cci.cst_id as customer_id, -- Unique numerical identifier assigned to each customer.
	cci.cst_key as customer_number,
	cci.cst_firstname as first_name,
	cci.cst_lastname as last_name,
	ela.cntry as country,
	cci.cst_marital_status as marital_status,
	case when cci.cst_gndr !='n/a' then cci.cst_gndr -- Take the gender from CRM
		 else coalesce(eca.gen,'n/a')
	end as gender,
	eca.bdate as birthdate,
	cci.cst_create_date as create_date -- The date and time when the customer record was created in the system

from `my-project-demo-dwh.dwh_silver.crm_cust_info` cci

    left join `my-project-demo-dwh.dwh_silver.erp_cust_az12` eca
        on cci.cst_key = eca.cid

    left join `my-project-demo-dwh.dwh_silver.erp_loc_a101` ela
        on cci.cst_key = ela.cid;

-- =============================================================================
-- Création Table de Dimension: gold.dim_products
-- =============================================================================

-- Provides information about the products and their attributes. 

CREATE OR REPLACE VIEW `my-project-demo-dwh.dwh_gold.dim_products` AS
SELECT
	row_number() over(order by cpi.prd_start_dt, cpi.prd_key) as product_key, -- Surrogate key uniquely identifying each product record in the product dimension table.
	cpi.prd_id as product_id, -- A unique identifier assigned to the product for internal tracking and referencing.
	cpi.prd_key as product_number, -- A structured alphanumeric code representing the product, often used for categorization or inventory.
	cpi.prd_nm as product_name,
	cpi.cat_id as category_id,
	epc.cat as category,
	epc.subcat as subcategory,
	epc.maintenance, -- Indicates whether the product requires maintenance
	cpi.prd_cost as cost,
	cpi.prd_line as product_line,
	cpi.prd_start_dt as start_date -- The date when the product became available for sale or use

from `my-project-demo-dwh.dwh_silver.crm_prd_info` cpi

    left join `my-project-demo-dwh.dwh_silver.erp_px_cat_g1v2` epc
        on cpi.cat_id = epc.id

where cpi.prd_end_dt is null; -- Keeping only current products by filtering historical data
