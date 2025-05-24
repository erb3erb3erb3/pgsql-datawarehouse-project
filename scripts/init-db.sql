/*

=========================================
Create Database and Schemas
=========================================

Script Purpose:
	This script creates a new database named 'DataWarehouse' after checking if it already exists.
	If the database exists it is dropped and recreated. The script also creates three schemas within
	the databse: 'bronze', 'silver', and 'gold'.
	
	PGAdmin Instrucitons:
		-You must be connected to postgres db to disconnect users and drop/recreate the Database.
	
		-You must be connected to DataWarehouse db to create the schemas.
	
WARNING:
	Running this script will drop the entire 'DataWarehouse' database if it exists. Proceed with caution
	and ensure you have proper backups before running this script.

*/


---*** Run connected to postgres database ***---
-- disconnect users
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'DataWarehouse';

-- drop and recreate the database DataWarehouse
DROP DATABASE IF EXISTS "DataWarehouse";

CREATE DATABASE "DataWarehouse";
