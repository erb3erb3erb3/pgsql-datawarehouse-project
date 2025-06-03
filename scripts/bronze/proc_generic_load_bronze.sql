CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
BEGIN
	--- Transaction block ---
	BEGIN
		--- Truncate and load crm_cust_info table from cust_info source csv file ---
		TRUNCATE TABLE bronze.crm_cust_info;

		COPY bronze.crm_cust_info 
		FROM '/Landing/Directory/source_crm/cust_info.csv'
		DELIMITER ',' CSV HEADER;
		
		
		--- Truncate and load crm_cust_info table from prd_info source csv file ---
		TRUNCATE TABLE bronze.crm_prd_info;
		
		COPY bronze.crm_prd_info 
		FROM '/Landing/Directory/source_crm/prd_info.csv'
		DELIMITER ',' CSV HEADER;
		
		
		
		--- Truncate and load crm_sales_details table from cust_info source csv file ---
		TRUNCATE TABLE bronze.crm_sales_details;
		
		COPY bronze.crm_sales_details
		FROM '/Landing/Directory/source_crm/sales_details.csv'
		DELIMITER ',' CSV HEADER;
		
		
		--- Truncate and load erp_cust_az12 table from cust_info source csv file ---
		TRUNCATE TABLE bronze.erp_cust_az12;
		
		COPY bronze.erp_cust_az12
		FROM '/Landing/Directory/source_erp/cust_az12.csv'
		DELIMITER ',' CSV HEADER;
		
		
		--- Truncate and load erp_loc_a101 table from cust_info source csv file ---
		TRUNCATE TABLE bronze.erp_loc_a101;
		
		COPY bronze.erp_loc_a101
		FROM '/Landing/Directory/source_erp/loc_a101.csv'
		DELIMITER ',' CSV HEADER;
		
		
		
		--- Truncate and load erp_px_cat_g1v2 table from cust_info source csv file ---
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		
		COPY bronze.erp_px_cat_g1v2
		FROM '/Landing/Directory/source_erp/px_cat_g1v2.csv'
		DELIMITER ',' CSV HEADER;

	--- plpgsql catch all exception handling to ensure Atomicity ---
	EXCEPTION WHEN OTHERS THEN
    	-- Rollback on any error
    	ROLLBACK;
    	RAISE NOTICE 'Error occurred: %, rolling back.', SQLERRM;
    	RAISE;  -- re-throw error
	END;
END;
$$;