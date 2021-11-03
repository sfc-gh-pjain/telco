USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS DEMO;
CREATE SCHEMA IF NOT EXISTS DEMO.TELCO;
USE SCHEMA DEMO.TELCO;

CREATE WAREHOUSE IF NOT EXISTS LAB_L_WH 
    WITH WAREHOUSE_SIZE = 'LARGE' 
    WAREHOUSE_TYPE = 'STANDARD' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 3 
    SCALING_POLICY = 'STANDARD' 
    COMMENT = 'WAREHOUSE CREATED FOR DEMO';

USE WAREHOUSE LAB_L_WH;

CREATE STAGE IF NOT EXISTS data_upload;
CREATE STAGE IF NOT EXISTS unstructured_dependency_jars;
CREATE STAGE IF NOT EXISTS unstructured_customer_data;

-- Update the location of the files as per your directory
PUT file:///Users/<USERNAME>/telco/data/telco_data.csv @data_upload overwrite=true;
PUT file:///Users/<USERNAME>/telco/data/schema_detection.parquet @data_upload overwrite=true;
PUT file:///Users/<USERNAME>/telco/data/001_email.eml @unstructured_customer_data auto_compress = false overwrite=true;
PUT file:///Users/<USERNAME>/telco/data/002_email.eml @unstructured_customer_data auto_compress = false overwrite=true;
PUT file:///Users/<USERNAME>/telco/data/LogEmailResponse.jar @unstructured_dependency_jars auto_compress = false overwrite=true;
PUT file:///Users/<USERNAME>/telco/data/tika-app-2.0.0.jar @unstructured_dependency_jars auto_compress = false overwrite=true;


CREATE TABLE IF NOT EXISTS PUBLIC.RAW_PARQUET_TELCO (
	CUSTOMERID VARCHAR(16777216),
	COUNTRY VARCHAR(16777216),
	CITY VARCHAR(16777216),
	"PHONE SERVICE" VARCHAR(16777216),
	"MULTIPLE LINES" VARCHAR(16777216),
	LATITUDE VARCHAR(16777216),
	"ONLINE SECURITY" VARCHAR(16777216),
	"SENIOR CITIZEN" VARCHAR(16777216),
	"MONTHLY CHARGES" VARCHAR(16777216),
	"STREAMING MOVIES" VARCHAR(16777216),
	"PAYMENT METHOD" VARCHAR(16777216),
	"LAT LONG" VARCHAR(16777216),
	"TENURE MONTHS" VARCHAR(16777216),
	COUNT VARCHAR(16777216),
	"PAPERLESS BILLING" VARCHAR(16777216),
	"CHURN VALUE" VARCHAR(16777216),
	"TECH SUPPORT" VARCHAR(16777216),
	"CHURN LABEL" VARCHAR(16777216),
	PARTNER VARCHAR(16777216),
	DEPENDENTS VARCHAR(16777216),
	"INTERNET SERVICE" VARCHAR(16777216),
	"STREAMING TV" VARCHAR(16777216),
	CONTRACT VARCHAR(16777216),
	"CHURN SCORE" VARCHAR(16777216),
	LONGITUDE VARCHAR(16777216),
	GENDER VARCHAR(16777216),
	"ONLINE BACKUP" VARCHAR(16777216),
	"DEVICE PROTECTION" VARCHAR(16777216),
	"TOTAL CHARGES" VARCHAR(16777216),
	CLTV VARCHAR(16777216),
	"CHURN REASON" VARCHAR(16777216),
	STATE VARCHAR(16777216),
	"ZIP CODE" VARCHAR(16777216)
);    

CREATE FILE FORMAT IF NOT EXISTS PUBLIC.TELCO_CSV_FORMAT 
    TYPE = 'CSV' COMPRESSION = 'AUTO' FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n' SKIP_HEADER = 1 
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    TRIM_SPACE = FALSE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
    ESCAPE = 'NONE' ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('\\N');


TRUNCATE TABLE PUBLIC.RAW_PARQUET_TELCO;

COPY INTO PUBLIC.RAW_PARQUET_TELCO from @data_upload/telco_data.csv.gz
FILE_FORMAT  = (FORMAT_NAME = 'PUBLIC.TELCO_CSV_FORMAT')
; 
