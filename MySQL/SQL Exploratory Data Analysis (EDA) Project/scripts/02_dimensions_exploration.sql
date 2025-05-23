
/*
===============================================================================
Dimensions Exploration
===============================================================================
Purpose:
    - To explore the structure of dimension tables.
	
SQL Functions Used:
    - DISTINCT
    - ORDER BY
===============================================================================
*/

-- Retrieve columns for products dimension
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'dim_products' OR TABLE_NAME = 'dim_customers';

-- Retrieve a list of unique countries from which customers originate
SELECT DISTINCT country
FROM gold.dim_customers
ORDER BY country;


-- Retrieve a list of unique categories, subcategories, and products
SELECT DISTINCT 
category,
subcategory,
product_name
FROM gold.dim_products
ORDER BY category, subcategory, product_name
