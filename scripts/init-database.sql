/*

=======================================
Create Database and Schemas
=======================================

Script Purpose:
	creates a database named 'DataWarehouse' after checking if exists.
	The script also sets up three schemas: 'bronze', 'silver', 'gold'
	
WARNING:
	First create the database and change the connection to the recently created databse
	and run the creation of the schemas
*/
DO $$
BEGIN
	IF NOT EXISTS(
		SELECT FROM pg_database WHERE datname = 'datawarehouse'
	) THEN CREATE DATABASE DataWarehouse;
	END IF;
END $$;

-- Creating Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


