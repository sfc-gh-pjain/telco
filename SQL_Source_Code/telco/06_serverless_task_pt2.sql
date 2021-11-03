use role sysadmin;
use schema demo.telco;
use warehouse lab_l_wh;


create or replace task task_update_feature_store
schedule = '1 minute'
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
COMMENT = 'Using serverless task to manage and update feature store'
AS 
call demo.telco.ordinalEncSQL('DEMO','TELCO','FEATURE_STORE','techsupport');

show tasks;

-- Pause or Resume tasks
alter task task_update_feature_store resume;
alter task task_update_feature_store suspend;

-- Check Task status
select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
    result_limit => 10,
    task_name=>'task_update_feature_store'));

-- Check final output
select * from feature_store;
SELECT * FROM FEATURE_METADATA;


