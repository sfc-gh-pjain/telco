USE ROLE SYSADMIN;
USE SCHEMA DEMO.TELCO;
USE WAREHOUSE LAB_L_WH;

CREATE OR REPLACE TABLE FEATURE_STORE AS SELECT * FROM SERVICES;
SELECT * FROM FEATURE_STORE;

create or replace task task_update_feature_store
schedule = '1 minute'
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
COMMENT = 'Using serverless task to manage and update feature store'
AS 
call demo.telco.ordinalEncSQL('DEMO','TELCO','FEATURE_STORE','techsupport');

SHOW TASKS;

-- Pause or Resume tasks
ALTER TASK TASK_UPDATE_FEATURE_STORE RESUME;
ALTER TASK TASK_UPDATE_FEATURE_STORE SUSPEND;

-- Check Task status
select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
    result_limit => 10,
    task_name=>'task_update_feature_store'));

-- Check final output
SELECT * FROM FEATURE_STORE;
SELECT * FROM FEATURE_METADATA;

-- At the end of the lab remember to stop your task from running (otherwise it will continue to execute every minute)
ALTER TASK TASK_UPDATE_FEATURE_STORE SUSPEND;
SHOW TASKS; --to verify the state is suspended
