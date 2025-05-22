SELECT TOP 100 * FROM silver.crm_cust_info;
SELECT TOP 100 * FROM silver.crm_prd_info;
SELECT TOP 100 * FROM silver.crm_sales_details;
SELECT TOP 100 * FROM silver.erp_cust_az12;
SELECT TOP 100 * FROM silver.erp_loc_a101;
SELECT TOP 100 * FROM silver.erp_px_cat_g1v2;

---------------------------------------------------
-- Checking silver.crm_cust_info
---------------------------------------------------

-- Check for Nulls or Duplicates in Primary Key
SELECT * FROM silver.crm_cust_info;
SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces
SELECT
cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT
cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Data standardization and Consistency
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

---------------------------------------------------
-- Checking silver.crm_prd_info
---------------------------------------------------

-- Check for unwanted Spaces
SELECT 
prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLS or negative numbers
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Data standardization and Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check for invalid date orders, cast datetime to date
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);


---------------------------------------------------
-- Checking silver.crm_sales_details
---------------------------------------------------

-- Check for invalid dates
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check for 
-- Sales = quantity * price
-- Negative, zeros or nulls not allowed

SELECT *
FROM silver.crm_sales_details
WHERE sls_sales != (sls_quantity * sls_price)
OR sls_sales <= 0
OR sls_sales IS NULL

SELECT *
FROM silver.crm_sales_details
WHERE sls_quantity <= 0
OR sls_quantity IS NULL

SELECT *
FROM silver.crm_sales_details
WHERE sls_price <= 0
OR sls_price IS NULL

---------------------------------------------------
-- Checking silver.erp_cust_az12
---------------------------------------------------
-- Check for cid nulls
SELECT
cid,
bdate,
gen
FROM silver.erp_cust_az12
WHERE cid = NULL

-- Keep the part of cid that is needed to connect with crm_cust_info table
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	 ELSE cid
END AS cid,
bdate,
gen
FROM silver.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	 ELSE cid
END NOT IN (SELECT cst_key FROM silver.crm_cust_info)

-- Check for bdate
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	 ELSE cid
END AS cid,
bdate,
gen
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Data standardization and Consistency
SELECT DISTINCT gen
FROM silver.erp_cust_az12

---------------------------------------------------
-- Checking silver.erp_loc_a101
---------------------------------------------------
-- Check for NULL cid
SELECT * FROM silver.erp_loc_a101
WHERE cid = NULL

-- Check for different length of cid
SELECT * FROM silver.erp_loc_a101
WHERE LEN(cid) != 10

-- Check countries
SELECT DISTINCT cntry 
FROM silver.erp_loc_a101
ORDER BY cntry

---------------------------------------------------
-- Checking silver.erp_px_cat_g1v2
---------------------------------------------------

-- Check for Unwanted Spaces
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data standardization and Consistency
SELECT DISTINCT cat
FROM silver.erp_px_cat_g1v2

SELECT DISTINCT subcat
FROM silver.erp_px_cat_g1v2

SELECT DISTINCT maintenance
FROM silver.erp_px_cat_g1v2
