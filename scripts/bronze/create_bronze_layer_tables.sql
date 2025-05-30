/*

=======================================
Create Database for the Bronze Layer
=======================================

Script Purpose:
	creates the bronze layer tables from the crm and erp source. Verifys if the 
	table exists and  drop the table than recreate the table
	
WARNING:
	This script will drop all the tables and than recreates.
*/


DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
	cst_id INTEGER,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_marital_status VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id INTEGER,
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INTEGER,
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INTEGER,
	sls_order_dt INTEGER,
	sls_ship_dt INTEGER,
	sls_due_dt INTEGER,
	sls_sales INTEGER,
	sls_quantity INTEGER,
	sls_price INTEGER
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
	cid VARCHAR(50),
	bdate DATE,
	gen VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
	cid VARCHAR(50),
	cntry VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
	id VARCHAR(50),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintence VARCHAR(50)
);


