USE ROLE SYSADMIN;
USE SCHEMA DEMO.TELCO;
USE WAREHOUSE LAB_L_WH;

-- SCHEMA DETECTION is the new feature in snowflake. Use INFER_SCHEMA table function TO CREATE TABLE

CREATE FILE FORMAT MY_PARQUET_FORMAT
  TYPE = PARQUET;

CREATE
    TABLE IF NOT EXISTS DEMO.TELCO.RAW_PARQUET USING TEMPLATE (
        SELECT
            ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM
            TABLE(
                INFER_SCHEMA(
                    LOCATION => '@data_upload/schema_detection.parquet',
                    FILE_FORMAT => 'MY_PARQUET_FORMAT'
                )
            )
    );
    
