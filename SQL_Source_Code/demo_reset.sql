/*
Created By: Parag Jain
Created For: Customer Churn analysis Data Pipeline

This script should be run after the demo is completed.
It truncates the required table and set's up the 

*/


USE ROLE SYSADMIN;
use schema DEMO.TELCO;
USE WAREHOUSE LAB_L_WH;

TRUNCATE TABLE SERVICES;
TRUNCATE TABLE LOCATION;
TRUNCATE TABLE DEMOGRAPHICS;
TRUNCATE TABLE STATUS;
TRUNCATE TABLE RAW_PARQUET;
DROP TABLE FEATURE_STORE;

-- BEFORE Starting Streaming simulation ---

alter pipe TELCO_PIPE set pipe_execution_paused = true;