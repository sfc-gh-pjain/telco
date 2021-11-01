use role SYSADMIN;
use schema DEMO.TELCO;
use warehouse LAB_L_WH;

-- Create Stage to load unstructured data (RFC822 format email)
create stage if not exists unstructured_customer_data
encryption = (type = 'snowflake_sse')
directory = (enable = true);


-- Stage to store the jar files for user defined function
create stage if not exists unstructured_dependency_jars;


-- Put the files in the stage using snowpark or snowsql or any other connector
-- Check the uploaded emails and jar files
ls @unstructured_customer_data;
ls @unstructured_dependency_jars;


-- Refresh Stage and check directory
alter stage unstructured_customer_data refresh;
select * from directory(@unstructured_customer_data);


-- Create a Java UDF to map with the uploaded jar file
create function if not exists parseEmail(file string)
returns string
language java
imports = ('@unstructured_dependency_jars/LogEmailResponse.jar', '@unstructured_dependency_jars/tika-app-2.0.0.jar')
HANDLER = 'LogEmailResponse.LogEmail'
;

select parseEmail('@unstructured_customer_data/002_email.eml') as V1;


-- Create a table to store the email data extracted using UDF
create or replace table customer_email (
  email_from varchar(200),
  email_subject varchar(200),
  email_body varchar,
  email_ret_date timestamp_ntz
  
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
