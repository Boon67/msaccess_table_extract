
/*https://docs.snowflake.com/en/sql-reference/sql/execute-job-service*/
/*https://docs.snowflake.com/en/user-guide/tasks-intro */
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
use schema IDENTIFIER($schema_name);
SET job_service_name=$schema_name || '.' || $SERVICE_NAME;
SET TASK_NAME=$schema_name || '.msdb_extract_task';

EXECUTE JOB SERVICE --Job Service definition
  IN COMPUTE POOL IDENTIFIER($compute_pool_name)
  NAME=IDENTIFIER($job_service_name)
  FROM SPECIFICATION $$
    spec:
      containers:
      - name: msaccessextract
        image: /msaccess/data/container_repository/msaccess_job_runner:latest
    $$; 

/* Option to create it as a periodic task 
CREATE OR REPLACE TASK IDENTIFIER($task_name) --Task to run
    SCHEDULE = 'USING CRON 0 * * * * UTC'  -- Runs at the beginning of every hour (0th minute).  Adjust as needed.
    AS
EXECUTE JOB SERVICE --Job Service definition
  IN COMPUTE POOL IDENTIFIER($compute_pool_name)
  NAME=IDENTIFIER($job_service_name)
  FROM SPECIFICATION $$
    spec:
      containers:
      - name: msaccessextract
        image: /msaccess/data/container_repository/msaccess_job_runner:latest
    $$; 

ALTER TASK IDENTIFIER($task_name) RESUME; -- Start the task
SHOW TASKS;
*/


SHOW TASKS;
--DROP TASK MSDB_EXTRACT_TASK;
