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
SET SERVICE_NAME='ms_access_extract';
SET IMAGE_ENDPOINT='sfsenorthamerica-tboon-aws2.registry.snowflakecomputing.com/msaccess/data/container_repository';
SET IMAGENAME='msaccess_job_runner';


EXECUTE JOB SERVICE
  IN COMPUTE POOL IDENTIFIER($compute_pool_name)
  NAME=MSACCESS.DATA.ms_access_extract
  FROM SPECIFICATION 
    $$
    spec:
      containers:
      - name: msaccessextract
        image: /msaccess/data/container_repository/msaccess_job_runner:latest
    $$; 

SHOW SERVICE CONTAINERS IN SERVICE MSACCESS.DATA.ms_access_extract;