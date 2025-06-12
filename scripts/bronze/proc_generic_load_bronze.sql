/*
===============================================================================
Stored Procedure: Load Bronze Layer Tables (Source -> Bronze)
===============================================================================
Script Purpose:
	This stored procedure loads data into the bronze schema from external CSV files.
	-Truncates the bronze tables before loading data.
	-Uses the COPY command to load data from the csv files to tables.

Parameters:
	N/A

Usage Example:
	CALL bronze.load_bronze();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
	found_rows INTEGER;
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	batch_start TIMESTAMP;
	batch_end TIMESTAMP;
BEGIN
	--- Transaction block ---
	BEGIN
		--- Declare batch_start to be used for batch duration calculation ---
		batch_start := clock_timestamp();
		RAISE NOTICE '========================================';
		RAISE NOTICE 'Loading the Bronze Layer';
		RAISE NOTICE '========================================';
		
		
		RAISE NOTICE '----------------------------------------';
		RAISE NOTICE 'Loading CRM Tables';
		RAISE NOTICE '----------------------------------------';
		
		--- Set start_time to calculate load duration ---
		start_time := clock_timestamp();
		
		--- Truncate and load crm_cust_info table from cust_info source csv file ---
		RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		
		RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
		COPY bronze.crm_cust_info 
		FROM '/Landing/Directory/source_crm/cust_info.csv'
		DELIMITER ',' CSV HEADER;
		GET DIAGNOSTICS found_rows = ROW_COUNT;
		RAISE NOTICE 'Rows affected: %', found_rows;
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: %', AGE(end_time, start_time);
		
		
		--- Set start_time to calculate crm_prd_info load duration ---
		start_time := clock_timestamp();
		
		--- Truncate and load crm_prd_info table from prd_info source csv file ---
		RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		
		RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
		COPY bronze.crm_prd_info 
		FROM '/Landing/Directory/source_crm/prd_info.csv'
		DELIMITER ',' CSV HEADER;
		GET DIAGNOSTICS found_rows = ROW_COUNT;
		RAISE NOTICE 'Rows affected: %', found_rows;
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: %', AGE(end_time, start_time);
		
		
		--- Set start_time to calculate crm_sales_details load duration ---
		start_time := clock_timestamp();
		--- Truncate and load crm_sales_details table from cust_info source csv file ---
		RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		
		RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
		COPY bronze.crm_sales_details
		FROM '/Landing/Directory/source_crm/sales_details.csv'
		DELIMITER ',' CSV HEADER;
		GET DIAGNOSTICS found_rows = ROW_COUNT;
		RAISE NOTICE 'Rows affected: %', found_rows;
		--- Declare end time and calculate load duration ---
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: %', AGE(end_time, start_time);
		
		RAISE NOTICE '----------------------------------------';
		RAISE NOTICE 'Loading ERP Tables';
		RAISE NOTICE '----------------------------------------';
		
		
		--- Set start_time to calculate erp_cust_az12 load duration ---
		start_time := clock_timestamp();
		
		--- Truncate and load erp_cust_az12 table from cust_info source csv file ---
		RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		
		RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
		COPY bronze.erp_cust_az12
		FROM '/Landing/Directory/source_erp/cust_az12.csv'
		DELIMITER ',' CSV HEADER;
		GET DIAGNOSTICS found_rows = ROW_COUNT;
		RAISE NOTICE 'Rows affected: %', found_rows;
		--- Declare end time and calculate load duration ---
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: %', AGE(end_time, start_time);
		
		
		--- Set start_time to calculate erp_loc_a101 load duration ---
		start_time := clock_timestamp();
		
		--- Truncate and load erp_loc_a101 table from cust_info source csv file ---
		RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
				
		RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
		COPY bronze.erp_loc_a101
		FROM '/Landing/Directory/source_erp/loc_a101.csv'
		DELIMITER ',' CSV HEADER;
		GET DIAGNOSTICS found_rows = ROW_COUNT;
		RAISE NOTICE 'Rows affected: %', found_rows;
		
		--- Declare end time and calculate load duration ---
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: %', AGE(end_time, start_time);
		
		
		--- Set start_time to calculate erp_px_cat_g1v2 load duration ---
		start_time := clock_timestamp();
		--- Truncate and load erp_px_cat_g1v2 table from cust_info source csv file ---
		RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		
		RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		COPY bronze.erp_px_cat_g1v2
		FROM '/Landing/Directory/source_erp/px_cat_g1v2.csv'
		DELIMITER ',' CSV HEADER;
		GET DIAGNOSTICS found_rows = ROW_COUNT;
		RAISE NOTICE 'Rows affected: %', found_rows;
		
		--- Declare end time and calculate load duration ---
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: %', AGE(end_time, start_time);
		
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
