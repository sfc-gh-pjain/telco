use role SYSADMIN;
use schema DEMO.TELCO;
use warehouse LAB_L_WH;

-- SCHEMA DETECTION is the new feature in snowflake. Use INFER_SCHEMA table function TO CREATE TABLE


CREATE
    table if not exists DMEO.TELCO.RAW_PARQUET using template (
        select
            array_agg(object_construct(*))
        from
            table(
                INFER_SCHEMA(
                    location => '@telco_data_stream/upload/telco_customer7_0_0_0.snappy.parquet',
                    file_format => 'my_parquet_format'
                )
            )
    );
    




select
    array_agg(object_construct(*))
from
    table(
        INFER_SCHEMA(
            location => '@telco_data_stream/upload/telco_customer7_0_0_0.snappy.parquet',
            file_format => 'my_parquet_format'
        )
    );
