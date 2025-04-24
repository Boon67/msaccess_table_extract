//This is a sample SQL script automates the setup of a dedicated Snowflake database environment for running job containers in SPCS.
//The intent is to creating a database (MSACCESS), a schema (DATA), a small warehouse //(demo_wh), and an administrative role 
//(MSACCESS_ROLE) with comprehensive privileges. It also optionally cleans up existing objects, 
//Account level permissing sare required to creates a compute pool (demo_compute_pool), and an image repository (container_repository).
//It is not meant to be all encompansing for environments, but a blueprint to map to your environment.


-- Define variables for database, schema, and role names
SET ACCOUNTLEVELPERMISSION='accountadmin'; --Used to create the compute pool
SET database_name = 'MSACCESS'; --DB Name
SET schema_name = $database_name || '.' || 'DATA'; --Schema
SET warehouse_name = 'demo_wh'; --Warehouse
SET db_admin_role_name = $database_name || '_ROLE';
SET db_admin_account_name='DEMO_ACCESS_USER';
SET image_repository_name='container_repository';
SET compute_pool_name='demo_compute_pool';
SET raw_stage='raw';
SET processing_stage='processing';
SET error_stage='error';
SET complete_stage='complete';

------------------------------------DATABASE CLEANUP (if required)-------------------------------
USE ROLE SYSADMIN;
--Cleanup Scripts if Required
DROP DATABASE IF EXISTS IDENTIFIER($database_name);
DROP WAREHOUSE IF EXISTS IDENTIFIER($warehouse_name);
DROP ROLE IF EXISTS IDENTIFIER($db_admin_role_name);

------------------------------------DB Setup and PermissionsRequirements--------------------------
USE ROLE SYSADMIN;
CREATE OR REPLACE WAREHOUSE IDENTIFIER($warehouse_name)
  WAREHOUSE_SIZE = 'XSMALL';

USE WAREHOUSE IDENTIFIER($warehouse_name);
-- Create the database
CREATE DATABASE IF NOT EXISTS IDENTIFIER($database_name);
-- Create the schema
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($schema_name);
USE SCHEMA IDENTIFIER($schema_name);

-----------------------------------Security Configuration Section---------------------------------
USE ROLE SECURITYADMIN; 
-- Create the admin user role
CREATE ROLE IF NOT EXISTS IDENTIFIER($db_admin_role_name);
-- Grant ownership of the roles to SYSADMIN
GRANT ROLE IDENTIFIER($db_admin_role_name) TO ROLE SYSADMIN; --Give permissions to SYSADMIN
GRANT ROLE IDENTIFIER($db_admin_role_name) TO USER IDENTIFIER($db_admin_account_name);

-- Grant privileges to the database admin role
GRANT ALL PRIVILEGES ON WAREHOUSE IDENTIFIER($warehouse_name) TO ROLE IDENTIFIER($db_admin_role_name);
GRANT ALL PRIVILEGES ON DATABASE IDENTIFIER($database_name) TO ROLE IDENTIFIER($db_admin_role_name);
GRANT ALL PRIVILEGES ON SCHEMA IDENTIFIER($schema_name) TO ROLE IDENTIFIER($db_admin_role_name);
GRANT USAGE ON WAREHOUSE IDENTIFIER($warehouse_name) TO IDENTIFIER($db_admin_role_name);;
GRANT USAGE ON DATABASE IDENTIFIER($database_name) TO IDENTIFIER($db_admin_role_name);;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA IDENTIFIER($schema_name) TO IDENTIFIER($db_admin_role_name);;
GRANT SELECT ON ALL TABLES IN SCHEMA IDENTIFIER($schema_name) TO IDENTIFIER($db_admin_role_name);;
GRANT CREATE STAGE ON SCHEMA IDENTIFIER($schema_name) TO ROLE IDENTIFIER($db_admin_role_name);
GRANT MONITOR, USAGE ON COMPUTE POOL IDENTIFIER($compute_pool_name) TO ROLE IDENTIFIER($db_admin_role_name);

------------------------------------Account Level Permission Requirements--------------------------------
USE ROLE IDENTIFIER($ACCOUNTLEVELPERMISSION);
DROP COMPUTE POOL if exists IDENTIFIER($compute_pool_name);
CREATE COMPUTE POOL IDENTIFIER($compute_pool_name) MIN_NODES = 1 MAX_NODES = 1 INSTANCE_FAMILY = 'CPU_X64_XS';
GRANT USAGE, MONITOR ON COMPUTE POOL IDENTIFIER($compute_pool_name) TO ROLE IDENTIFIER($db_admin_role_name);

DESCRIBE COMPUTE POOL IDENTIFIER($compute_pool_name);

------------------------------------USER Level Permission Requirements--------------------------------
USE ROLE IDENTIFIER($db_admin_role_name);
CREATE IMAGE REPOSITORY IDENTIFIER($image_repository_name);
SHOW IMAGE REPOSITORIES; --Need to capture the repository URL for uploading the container image
CREATE STAGE IF NOT EXISTS IDENTIFIER($RAW_STAGE);
CREATE STAGE IF NOT EXISTS IDENTIFIER($PROCESSING_STAGE);
CREATE STAGE IF NOT EXISTS IDENTIFIER($ERROR_STAGE);
CREATE STAGE IF NOT EXISTS IDENTIFIER($COMPLETE_STAGE);

