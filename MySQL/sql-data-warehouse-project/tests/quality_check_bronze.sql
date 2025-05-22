SELECT TOP 100 * FROM bronze.crm_cust_info;
SELECT TOP 100 * FROM bronze.crm_prd_info;
SELECT TOP 100 * FROM bronze.crm_sales_details;
SELECT TOP 100 * FROM bronze.erp_cust_az12;
SELECT TOP 100 * FROM bronze.erp_loc_a101;
SELECT TOP 100 * FROM bronze.erp_px_cat_g1v2;

------------------ Cleaning Data ------------------

---------------------------------------------------
-- Check bronze.crm_cust_info
---------------------------------------------------

SELECT TOP 100 * FROM bronze.crm_cust_info;

-- Check for Nulls or Duplicates in Primary Key

SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

SELECT
*
FROM (
SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
)t
WHERE flag_last = 1;

-- Check for unwanted spaces
SELECT
cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT
cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Trim names and if there are duplicates records for a customer keep the one that was created last
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
FROM (
SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
)t
WHERE flag_last = 1;

-- Data standardization and Consistency
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;


SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	ELSE 'n/a'
END cst_marital_status,
CASE
	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
)t
WHERE flag_last = 1 AND cst_id IS NOT NULL;

---------------------------------------------------
-- Checking bronze.crm_prd_info
---------------------------------------------------

-- Check for Nulls or Duplicates in Primary Key
SELECT 
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Create columns for category id and product key in order to be able to connect with other tables
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info;

-- Check for unwanted Spaces
SELECT 
prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLS or negative numbers
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Divide prd_key to two parts and format them in order to be able to connect with other tables
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info;

-- Data standardization and Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info;

-- Check for invalid date orders, cast datetime to date
SELECT *
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line,
CAST (prd_start_dt AS DATE) AS prd_start_dt,
CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;

---------------------------------------------------
-- Checking bronze.crm_sales_details
---------------------------------------------------

-- Check if keys for other tables can be found 
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
FROM bronze.crm_sales_details
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
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Check for invalid dates sls_order_dt
SELECT 
NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE
WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
ELSE CAST(CAST (sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details


-- Check for invalid dates sls_ship_dt
SELECT sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt = 0 
OR LEN(sls_ship_dt) != 8 
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101


SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE
WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
ELSE CAST(CAST (sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE
WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
ELSE CAST(CAST (sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details

-- Check for invalid dates sls_due_dt
SELECT sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt = 0 
OR LEN(sls_due_dt) != 8 
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE
WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
ELSE CAST(CAST (sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE
WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
ELSE CAST(CAST (sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE
WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
ELSE CAST(CAST (sls_due_dt AS VARCHAR) AS DATE)
END AS  sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details

-- Check for invalid dates
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check for 
-- Sales = quantity * price
-- Negative, zeros or nulls not allowed

SELECT *
FROM bronze.crm_sales_details
WHERE sls_sales != (sls_quantity * sls_price)
OR sls_sales <= 0
OR sls_sales IS NULL

SELECT *
FROM bronze.crm_sales_details
WHERE sls_quantity <= 0
OR sls_quantity IS NULL

SELECT *
FROM bronze.crm_sales_details
WHERE sls_price <= 0
OR sls_price IS NULL

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE
WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
ELSE CAST(CAST (sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE
WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
ELSE CAST(CAST (sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE
WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
ELSE CAST(CAST (sls_due_dt AS VARCHAR) AS DATE)
END AS  sls_due_dt,
CASE
WHEN sls_sales != (sls_quantity * ABS(sls_price)) OR sls_sales IS NULL OR sls_sales <= 0 THEN sls_quantity * ABS(sls_price)
ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE
WHEN sls_price < 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details

---------------------------------------------------
-- Checking bronze.erp_cust_az12
---------------------------------------------------

-- Check for cid nulls
SELECT
cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE cid = NULL

-- Keep the part of cid that is needed to connect with crm_cust_info table
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	 ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12
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
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()
-- Future birthdates found, replace them with NULL
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	 ELSE cid
END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL
	 ELSE bdate
END AS bdate,
gen
FROM bronze.erp_cust_az12

-- Data standardization and Consistency
SELECT DISTINCT gen
FROM bronze.erp_cust_az12

SELECT DISTINCT gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12


SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	 ELSE cid
END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL
	 ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

---------------------------------------------------
-- Checking bronze.erp_loc_a101
---------------------------------------------------

-- Check for NULL cid
SELECT * FROM bronze.erp_loc_a101
WHERE cid = NULL

-- Check for different length of cid
SELECT * FROM bronze.erp_loc_a101
WHERE LEN(cid) != 11

-- Transform cid
SELECT 
REPLACE(cid, '-','') AS cid,
cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-','') NOT IN (SELECT cst_key FROM silver.crm_cust_info)

-- Check countries
SELECT DISTINCT cntry AS old_cntry,
CASE WHEN TRIM(cntry) IN ('DE', 'Germany') THEN 'Germany'
	 WHEN TRIM(cntry) IN ('USA', 'US', 'United States') THEN 'United States'
	 WHEN TRIM(cntry) IS NULL OR cntry ='' THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

SELECT 
REPLACE(cid, '-','') AS cid,
CASE WHEN cntry IN ('DE', 'Germany') THEN 'Germany'
	 WHEN cntry IN ('USA', 'US', 'United States') THEN 'United States'
	 WHEN cntry IS NULL OR cntry ='' THEN 'n/a'
	 ELSE cntry
END AS cntry
FROM bronze.erp_loc_a101

---------------------------------------------------
-- Checking bronze.erp_px_cat_g1v2
---------------------------------------------------
SELECT * FROM bronze.erp_px_cat_g1v2
SELECT * FROM silver.crm_prd_info

-- Check for Unwanted Spaces
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data standardization and Consistency
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2

SELECT 
id,
TRIM(cat) AS cat,
TRIM(subcat) AS subcat,
TRIM(maintenance) AS maintenance
FROM bronze.erp_px_cat_g1v2

---------------------------------------------------
-- Insert data to silver layer
---------------------------------------------------
TRUNCATE TABLE silver.erp_px_cat_g1v2
INSERT INTO silver.erp_px_cat_g1v2(
id,
cat,
subcat,
maintenance
)

SELECT 
id,
TRIM(cat) AS cat,
TRIM(subcat) AS subcat,
TRIM(maintenance) AS maintenance
FROM bronze.erp_px_cat_g1v2