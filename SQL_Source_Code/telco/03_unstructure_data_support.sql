use role SYSADMIN;
use schema DEMO.TELCO;
use warehouse LAB_L_WH;

-- Create Stage to load unstructured data (RFC822 format email)
CREATE STAGE IF NOT EXISTS UNSTRUCTURED_CUSTOMER_DATA
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
DIRECTORY = (ENABLE = TRUE);


-- Stage to store the jar files for user defined function
CREATE STAGE IF NOT EXISTS unstructured_dependency_jars;
CREATE STAGE IF NOT EXISTS unstructured_customer_data;

-- Put the files in the stage using snowpark or snowsql or any other connector
-- Check the uploaded emails and jar files
ls @unstructured_customer_data;
ls @unstructured_dependency_jars;


-- Refresh Stage and check directory
alter stage unstructured_customer_data refresh;
select * from directory(@unstructured_customer_data);

-- Open Snowsql and upload the jar files and email files
-- put file:///Users/work/telco/data/001_email.eml @unstructured_customer_data auto_compress = false;
-- put file:///Users/work/telco/data/002_email.eml @unstructured_customer_data auto_compress = false;

-- put file:///Users/work/telco/data/LogEmailResponse.jar @unstructured_dependency_jars auto_compress = false;
-- put file:///Users/work/telco/data/tika-app-2.0.0.jar @unstructured_dependency_jars auto_compress = false;

-- Create a Java UDF to map with the uploaded jar file
create function if not exists parseEmail(file string)
returns string
language java
imports = ('@unstructured_dependency_jars/LogEmailResponse.jar', '@unstructured_dependency_jars/tika-app-2.0.0.jar')
HANDLER = 'LogEmailResponse.LogEmail'
;

select parseEmail('@unstructured_customer_data/001_email.eml') as V1;


-- Create a table to store the email data extracted using UDF
CREATE OR REPLACE TABLE CUSTOMER_EMAIL (
  EMAIL_FROM VARCHAR(200),
  EMAIL_SUBJECT VARCHAR(200),
  EMAIL_BODY VARCHAR,
  EMAIL_RET_DATE TIMESTAMP_NTZ
  
);

-- Insert the output of the function into the Customer Email table
insert into customer_email select e.email_from,e.email_subject,e.email_body,e.email_ret_date from
(
    select 
        GET(split(V1,'|'),0) as email_from,
        GET(split(V1,'|'),1) as email_subject,
        GET(split(V1,'|'),2) as email_body,
        current_timestamp() as email_ret_date 
  from (
        select parseEmail('@unstructured_customer_data/001_email.eml') as V1) 
) e;

insert into customer_email select e.email_from,e.email_subject,e.email_body,e.email_ret_date from
(
    select 
        GET(split(V1,'|'),0) as email_from,
        GET(split(V1,'|'),1) as email_subject,
        GET(split(V1,'|'),2) as email_body,
        current_timestamp() as email_ret_date 
  from (
        select parseEmail('@unstructured_customer_data/002_email.eml') as V1) 
) e;


select * from customer_email;


--OPTIONAL : remove processed data
--rm @unstructured_customer_data/002_email.eml;
