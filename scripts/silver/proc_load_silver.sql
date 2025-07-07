/*
===============================================================================
Stored Procedure: Load Silver Layer Tables (Bronze -> Silver)
===============================================================================
Script Purpose:
	This stored procedure performs the ETL process to populate the 'Silver' 
	schema tables from 'Bronze' schema.
		Actions Performed:
			-Truncates the silver tables before loading data.
			-Inserts Transformed and cleansed data from Bronze into 
			 Silver tables.
Parameters:
	N/A

Usage Example:
	CALL silver.load_silver();
===============================================================================
*/


CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE 
	found_rows INTEGER;
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	batch_start TIMESTAMP;
	batch_end TIMESTAMP;
BEGIN
----Transaction Block----
	BEGIN
		--- Declare batch_start to be used for batch duration calculation ---
		batch_start := clock_timestamp();
		
	
		/* 
		==========================================

		INSERT INTO SILVER.CRM_CUST_INFO

		==========================================
		*/
		--- Set start_time to calculate load duration ---
		start_time := clock_timestamp();
		
		RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE silver.crm_cust_info;

		RAISE NOTICE '>> Inserting Data Into Table: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(	
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
			ELSE 'N/A'
		END cst_marital_status,
		CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'N/A'
		END cst_gndr,
		cst_create_date
		FROM (
			SELECT *, 
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id NOTNULL
		) WHERE flag_last = 1;
		
		--- Calculating rows impacted and processesing time.
		GET DIAGNOSTICS found_rows = ROW_COUNT;
		RAISE NOTICE 'Rows affected: %', found_rows;
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: %', AGE(end_time, start_time);

		/* 
		==========================================

		INSERT INTO SILVER.CRM_PRD_INFO

		==========================================
		*/
		--- Set start_time to calculate load duration ---
		start_time := clock_timestamp();
		
		RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE silver.crm_prd_info;

		RAISE NOTICE '>> Inserting Data Into Table: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm ,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
		prd_id,
		REPLACE(substring(prd_key, 1, 5), '-', '_')	AS cat_id,
		SUBSTRING(prd_key, 7, LENGTH(prd_key)) as prd_key,
		TRIM(prd_nm) AS prd_nm,
		COALESCE(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'N/A'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info;

		--- Calculating rows impacted and processesing time.
		GET DIAGNOSTICS found_rows = ROW_COUNT;
		RAISE NOTICE 'Rows affected: %', found_rows;
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: %', AGE(end_time, start_time);

		/*
		=====================================================

		INSERT INTO SILVER.CRM_SALES_DETAILS

		=====================================================
		*/
		
		--- Set start_time to calculate load duration ---
		start_time := clock_timestamp();
		
		RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE silver.crm_sales_details;

		RAISE NOTICE '>> Inserting Data Into Table: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id, 
			sls_order_dt,
			sls_ship_dt, 
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price,
			dq_flag
		)

		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE
			WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
			ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
		END AS sls_order_dt_dt,
		CASE
			WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
			ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
		END AS sls_ship_dt,
		CASE
			WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
			ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
		END AS sls_due_dt,
		CASE 
			WHEN sls_sales <= 0 OR sls_sales ISNULL OR sls_sales != sls_quantity*ABS(sls_price) THEN sls_quantity*ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE 
			WHEN sls_price <= 0 OR sls_price ISNULL THEN sls_sales/NULLIF(sls_quantity, 0)
			ELSE sls_price
		END AS sls_price,
		CASE 
			WHEN COALESCE(sls_price, 0) = 0 AND COALESCE(sls_sales, 0) = 0 THEN 1
			ELSE 0
		END AS dq_flag
		FROM bronze.crm_sales_details;

		--- Calculating rows impacted and processesing time.
		GET DIAGNOSTICS found_rows = ROW_COUNT;
		RAISE NOTICE 'Rows affected: %', found_rows;
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: %', AGE(end_time, start_time);

		/*
		=========================================================================

		INSERT INTO SILVER.ERP_CUST_AZ12 

		=========================================================================
		*/

		--- Set start_time to calculate load duration ---
		start_time := clock_timestamp();
		
		RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE silver.erp_cust_az12;

		RAISE NOTICE '>> Inserting Data Into Table: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		select
		CASE 
			WHEN cid like 'NAS%' THEN SUBSTRING(cid,4)
			ELSE cid
		END AS cid,
		CASE
			WHEN bdate > NOW() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE
			WHEN UPPER(TRIM(gen)) in ('M', 'MALE') THEN 'Male' 
			WHEN UPPER(TRIM(gen)) in ('F', 'FEMALE') THEN 'Female'
			ELSE 'N/A'
		END AS gen
		from bronze.erp_cust_az12;

		--- Calculating rows impacted and processesing time.
		GET DIAGNOSTICS found_rows = ROW_COUNT;
		RAISE NOTICE 'Rows affected: %', found_rows;
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: %', AGE(end_time, start_time);

		/*
		=========================================================================

		INSERT INTO SILVER.ERP_LOC_A101

		=========================================================================
		*/

		--- Set start_time to calculate load duration ---
		start_time := clock_timestamp();
		
		RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE silver.erp_loc_a101;

		RAISE NOTICE '>> Inserting Data Into Table: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
		)

		select 
		TRANSLATE(cid, '-', '') as cid,
		CASE
			WHEN TRIM(UPPER(cntry)) in ('US', 'USA') THEN 'United States'
			WHEN TRIM(UPPER(cntry)) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) = '' OR cntry ISNULL THEN 'N/A'
			ELSE TRIM(cntry)
		END as cntry
		from bronze.erp_loc_a101;

		--- Calculating rows impacted and processesing time.
		GET DIAGNOSTICS found_rows = ROW_COUNT;
		RAISE NOTICE 'Rows affected: %', found_rows;
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: %', AGE(end_time, start_time);

		/*
		=========================================================================

		INSERT INTO SILVER.ERP_PX_CAT_G1V2

		=========================================================================
		*/
		
		--- Set start_time to calculate load duration ---
		start_time := clock_timestamp();		
		
		RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE silver.erp_px_cat_g1v2;

		RAISE NOTICE '>> Inserting Data Into Table: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		select 
		id, 
		cat,
		subcat, 
		maintenance 
		from bronze.erp_px_cat_g1v2;
		
		--- Calculating rows impacted and processesing time.
		GET DIAGNOSTICS found_rows = ROW_COUNT;
		RAISE NOTICE 'Rows affected: %', found_rows;
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: %', AGE(end_time, start_time);
		
		
		/*
		=========================================================================

		Batch Summary

		=========================================================================
		*/
		
		--- Declare batch_end and calculate batch duration ---
		batch_end := clock_timestamp();
		RAISE NOTICE '========================================';
		RAISE NOTICE 'Batch Load Completed.';
		RAISE NOTICE 'Total Batch Load Duration: %', AGE(batch_end, batch_start);
		RAISE NOTICE '========================================';

	--- plpgsql catch all exception handling to ensure Atomicity ---
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE '========================================';
		RAISE NOTICE 'Error occurred: %, rolling back.', SQLERRM;
		RAISE NOTICE '========================================';
		RAISE;  -- re-throw error
	END;
END;
$$;
