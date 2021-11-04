/* 

Created By: Parag Jain
Created For: Customer Churn analysis Data Pipeline

This is the setup step for the telecom churn analysis data pipeline
Its one time setup only. After running this and delivering a demo, 
you can use the demo_reset.sql script for resetting the demo and cleaning up
any data the you have uploaded into the models.

*/

USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS DEMO;
CREATE SCHEMA IF NOT EXISTS TELCO;
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

CREATE FILE FORMAT IF NOT EXISTS DEMO.PUBLIC.TELCO_CSV_FORMAT 
    TYPE = 'CSV' COMPRESSION = 'AUTO' FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n' SKIP_HEADER = 1 
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    TRIM_SPACE = FALSE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
    ESCAPE = 'NONE' ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('\\N');


-- Use PUT and copy command to load the data into RAW_PARQUET_TELCO table 
-- or just use direct upload from UI

CREATE STAGE IF NOT EXISTS data_upload;

-- Open Snowsql and upload the jar files and email files
-- https://docs.snowflake.com/en/user-guide/snowsql-start.html
-- >> snowsql -c <connection>
-- >> put file:///Users/<USERNAME>/telco/data/telco_data.csv @data_upload;
-- >> put file:///Users/<USERNAME>/telco/data/schema_detection.parquet @data_upload overwrite=true;

TRUNCATE TABLE PUBLIC.RAW_PARQUET_TELCO;

COPY INTO PUBLIC.RAW_PARQUET_TELCO from @data_upload/telco_data.csv.gz
FILE_FORMAT  = (FORMAT_NAME = 'PUBLIC.TELCO_CSV_FORMAT')
;  

CREATE OR REPLACE PROCEDURE "ONEHOTENCSQL"("DBNAME" VARCHAR(16777216), "SCHNAME" VARCHAR(16777216), "TBLNAME" VARCHAR(16777216), "COLNAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
    // Set context
  var tblName = DBNAME + "." + SCHNAME + "." + TBLNAME;
  var msg = "Info: OneHot encoding for column " + COLNAME + " completed";
  
  // list distinct column values
  var listSql = "select distinct " + COLNAME + " from " + tblName + " ;" ;
  var listDistinct = snowflake.execute({ sqlText: listSql });
  var counter = 0 ;
  
  while(listDistinct.next())
  {
      var col = listDistinct.getColumnValue(1);
      var colName = COLNAME + "_" + col.replace(/\\((.*?)\\)/g,'''').replace(/ /g,'''');

      //add encoded col
      var addColSql = "alter table " + tblName + " add column " + colName + " number;" ;
      try 
      {
          var addCol = snowflake.execute({ sqlText: addColSql });
          counter += 1;
      }
      catch(err)
      {
          msg += " \\n Info: Column ''" + colName + "'' already exists ";
 
      }

      //update encoded col
      var updColSql = "update " + tblName + " set " + colName + "= (case lower(" + COLNAME + ") when lower(''" + col +"'') then 1 else 0 end);";

      try 
      {
          var updCol = snowflake.execute({ sqlText: updColSql });
          counter += 1;
      }
      catch(err)
      {
          msg += " \\n Info: columns updated: " + counter + " /\\n Warning: " + err ;
      }
  }

    return msg;

';

CREATE OR REPLACE PROCEDURE "ORDINALENCSQL"("DBNAME" VARCHAR(16777216), "SCHNAME" VARCHAR(16777216), "TBLNAME" VARCHAR(16777216), "COLNAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
  var counter = 0 ;
  var msg = "Ordinal Encoding completed Successfully ";
    // Set context
  var tblName = DBNAME + "." + SCHNAME + "." + TBLNAME;

  // Create new ordinal encoding column
  var colName = COLNAME + "_ordinal";

  //add encoded col
  var addColSql = "alter table " + tblName + " add column " + colName + " number;" ;
  try 
  {
      var addCol = snowflake.execute({ sqlText: addColSql });
  }
  catch(err)
  {
      counter = 0;
      msg += " , Info: Column ''" + colName + "'' already exists ";
  }

  // list distinct column values
  var listSql = "select distinct "+ COLNAME + " from " + tblName + " ;" ;
  var listDistinct = snowflake.execute({ sqlText: listSql });
  
  while(listDistinct.next())
  {
      var col = listDistinct.getColumnValue(1);
      counter += 1;
      
      //update ordinal encoded col
      var updColSql = "update " + tblName + " set " + colName + " = " + counter + " where " + COLNAME + " = ''" + col +"'' ;";

      try 
      {
          var updCol = snowflake.execute({ sqlText: updColSql });
      }
      catch(err)
      {
          msg = "Warning: " + updColSql + " : " + err;
      }
  }

    return msg;

';