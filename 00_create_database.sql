/*

=======================================
Create Database and Schemas
=======================================

Script Purpose:
	creates a database named 'DataWarehouse' after checking if exists.
	The script also sets up three schemas: 'bronze', 'silver', 'gold'
*/
DO $$
BEGIN
	IF NOT EXISTS(
		SELECT FROM pg_database WHERE datname = 'datawarehouse'
	) THEN CREATE DATABASE DataWarehouse;
	END IF;
	
-- Creating Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

END $$;
