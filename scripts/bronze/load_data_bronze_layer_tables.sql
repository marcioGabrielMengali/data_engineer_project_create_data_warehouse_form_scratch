/*

=======================================
Load data to bronze layer tables
=======================================

Script Purpose:
	load data to the bronze layer tables
	
*/

CREATE OR REPLACE FUNCTION bronze.load_bronze() 
RETURNS VOID AS  $$
DECLARE
  rows_inserted INTEGER;
  t_start_batch_time TIMESTAMP;
  t_start TIMESTAMP;
  t_end TIMESTAMP;
  t_end_batch_time   TIMESTAMP;
BEGIN
	RAISE NOTICE '==============================';
	RAISE NOTICE 'Loading Bronze Layer';
	RAISE NOTICE '==============================';
	
	RAISE NOTICE '-----------------------------';
	RAISE NOTICE 'Loading Crm Tables';
	RAISE NOTICE '-----------------------------';
	
	t_start_batch_time := clock_timestamp();
	t_start := clock_timestamp();
	BEGIN 
		RAISE INFO '>> Truncating Table bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		RAISE INFO '>> Inserting Data Into: bronze.crm_cust_info';
		COPY bronze.crm_cust_info
		FROM '/docker-entrypoint-initdb.d/source_crm/cust_info.csv'
		DELIMITER ','
		CSV HEADER;
		GET DIAGNOSTICS rows_inserted = ROW_COUNT;
		RAISE NOTICE '>>> Inserted rows: %', rows_inserted;
	EXCEPTION
		WHEN OTHERS THEN RAISE EXCEPTION 'Error loading  bronze.crm_cust_info: %', SQLERRM;
	END;
	t_end := clock_timestamp();
	RAISE INFO '*********************************************************';
	RAISE NOTICE 'Load Duration Table bronze.crm_cust_info: % seconds', EXTRACT(SECOND FROM (t_end - t_start)) 
                                        + EXTRACT(MINUTE FROM (t_end - t_start)) * 60
                                        + EXTRACT(HOUR FROM (t_end - t_start)) * 3600;
	RAISE INFO '*********************************************************';
	
	t_start := clock_timestamp();
	BEGIN
		RAISE INFO '>> Truncating Table bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		RAISE INFO '>> Inserting Data Into: bronze.crm_prd_info';
		COPY bronze.crm_prd_info
		FROM '/docker-entrypoint-initdb.d/source_crm/prd_info.csv'
		DELIMITER ','
		CSV HEADER;
		GET DIAGNOSTICS rows_inserted = ROW_COUNT;
		RAISE NOTICE '>>> Inserted rows: %', rows_inserted;
	EXCEPTION
		WHEN OTHERS THEN RAISE EXCEPTION 'Error loading bronze.crm_prd_info: %', SQLERRM;
	END;
	t_end := clock_timestamp();
	RAISE INFO '*********************************************************';
	RAISE NOTICE 'Load Duration Table bronze.crm_prd_info: % seconds', EXTRACT(SECOND FROM (t_end - t_start)) 
                                        + EXTRACT(MINUTE FROM (t_end - t_start)) * 60
                                        + EXTRACT(HOUR FROM (t_end - t_start)) * 3600;
	RAISE INFO '*********************************************************';
	
	t_start := clock_timestamp();
	BEGIN
		RAISE INFO '>> Truncating Table bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		RAISE INFO '>> Inserting Data Into: bronze.crm_sales_details';
		COPY bronze.crm_sales_details
		FROM '/docker-entrypoint-initdb.d/source_crm/sales_details.csv'
		DELIMITER ','
		CSV HEADER;
		GET DIAGNOSTICS rows_inserted = ROW_COUNT;
		RAISE NOTICE '>>> Inserted rows: %', rows_inserted;
	EXCEPTION
		WHEN OTHERS THEN RAISE EXCEPTION 'Error loading bronze.crm_sales_details: %', SQLERRM;
	END;
	t_end := clock_timestamp();
	RAISE INFO '*********************************************************';
	RAISE NOTICE 'Load Duration Table bronze.crm_sales_details: % seconds', EXTRACT(SECOND FROM (t_end - t_start)) 
                                        + EXTRACT(MINUTE FROM (t_end - t_start)) * 60
                                        + EXTRACT(HOUR FROM (t_end - t_start)) * 3600;
	RAISE INFO '*********************************************************';

	RAISE NOTICE '-----------------------------';
	RAISE NOTICE 'Loading Erp Tables';
	RAISE NOTICE '-----------------------------';
	
	t_start := clock_timestamp();
	BEGIN
		RAISE INFO '>> Truncating Table bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		RAISE INFO '>> Inserting Data Into: bronze.erp_cust_az12';
		COPY bronze.erp_cust_az12
		FROM '/docker-entrypoint-initdb.d/source_erp/CUST_AZ12.csv'
		DELIMITER ','
		CSV HEADER;
		GET DIAGNOSTICS rows_inserted = ROW_COUNT;
		RAISE NOTICE '>>> Inserted rows: %', rows_inserted;
	EXCEPTION
		WHEN OTHERS THEN RAISE EXCEPTION 'Error loading bronze.erp_cust_az12: %', SQLERRM;
	END;
	t_end := clock_timestamp();
	RAISE INFO '*********************************************************';
	RAISE NOTICE 'Load Duration Table bronze.erp_cust_az12: % seconds', EXTRACT(SECOND FROM (t_end - t_start)) 
                                        + EXTRACT(MINUTE FROM (t_end - t_start)) * 60
                                        + EXTRACT(HOUR FROM (t_end - t_start)) * 3600;
	RAISE INFO '*********************************************************';
	
	t_start := clock_timestamp();
	BEGIN
		RAISE INFO '>> Truncating Table bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		RAISE INFO '>> Inserting Data Into: bronze.erp_loc_a101';
		COPY bronze.erp_loc_a101
		FROM '/docker-entrypoint-initdb.d/source_erp/LOC_A101.csv'
		DELIMITER ','
		CSV HEADER;
		GET DIAGNOSTICS rows_inserted = ROW_COUNT;
		RAISE NOTICE '>>> Inserted rows: %', rows_inserted;
	EXCEPTION
		WHEN OTHERS THEN RAISE EXCEPTION 'Error loading bronze.erp_loc_a101: %', SQLERRM;
	END;
	t_end := clock_timestamp();
	RAISE INFO '*********************************************************';
	RAISE NOTICE 'Load Duration Table bronze.erp_loc_a101: % seconds', EXTRACT(SECOND FROM (t_end - t_start)) 
                                        + EXTRACT(MINUTE FROM (t_end - t_start)) * 60
                                        + EXTRACT(HOUR FROM (t_end - t_start)) * 3600;
	RAISE INFO '*********************************************************';

	t_start := clock_timestamp();
	BEGIN
		RAISE INFO '>> Truncating Table bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		RAISE INFO '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		COPY bronze.erp_px_cat_g1v2
		FROM '/docker-entrypoint-initdb.d/source_erp/PX_CAT_G1V2.csv'
		DELIMITER ','
		CSV HEADER;
		GET DIAGNOSTICS rows_inserted = ROW_COUNT;
		RAISE NOTICE '>>> Inserted rows: %', rows_inserted;
	EXCEPTION
		WHEN OTHERS THEN RAISE EXCEPTION 'Error loading bronze.erp_px_cat_g1v2: %', SQLERRM;
	END;
	t_end := clock_timestamp();
	RAISE INFO '*********************************************************';
	RAISE NOTICE 'Load Duration Table bronze.erp_px_cat_g1v2: % seconds', EXTRACT(SECOND FROM (t_end - t_start)) 
                                        + EXTRACT(MINUTE FROM (t_end - t_start)) * 60
                                        + EXTRACT(HOUR FROM (t_end - t_start)) * 3600;
	RAISE INFO '*********************************************************';
	t_end_batch_time := clock_timestamp();
	RAISE NOTICE '=========================================';
	RAISE NOTICE 'Loading Bronze Layer is Completed';
	RAISE NOTICE '=========================================';
	RAISE INFO '*********************************************************';
	RAISE NOTICE 'Total Load Duration: % seconds', EXTRACT(SECOND FROM (t_end_batch_time - t_start_batch_time)) 
                                        + EXTRACT(MINUTE FROM (t_end_batch_time - t_start_batch_time)) * 60
                                        + EXTRACT(HOUR FROM (t_end_batch_time - t_start_batch_time)) * 3600;
	RAISE INFO '*********************************************************';
	
END;
$$ LANGUAGE plpgsql;


SELECT bronze.load_bronze();
