/*
===============================================================================
Gold Quality Checks
===============================================================================
Script Purpose:
    This script performs a series of data quality checks on the gold layer views to ensure the integrity, 
    completeness, and consistency of critical data elements.
    These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Run this script after creating the 'Gold layer' views.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/


/*
====================================================================
gold.dim_customers
====================================================================
*/
-- Check for uniqueness of customer_key
-- Expectation: No results 
SELECT customer_key, COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

/*
====================================================================
gold.product_key
====================================================================
*/
-- Check for uniqueness of product_key
-- Expectation: No results 
SELECT product_key, COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

/*
====================================================================
gold.fact_sales'
====================================================================
*/
-- Check the connectivity between fact and dimension tables
-- Expectation: No Results
SELECT * 
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc
ON fs.customer_key = dc.customer_key
LEFT JOIN gold.dim_products dp
ON fs.product_key = dp.product_key
WHERE dc.customer_key IS NULL OR dp.product_key IS NULL  
