# Setup

There are two pieces of this demo. One that needs to be run on the snowflake worksheet and other that needs to be run on the snowpark jupyter notebook

## SQL setup and steps

**Best way to show this demo is setup each SQL script in a separate worksheet in your demo environment

1. Run the environment setup SQL in worksheet in your demo env. You can also use the "snowsql_upload.sql" with the snowsql client as:

```
$ cd <path to telco parent directory>
$ snowsql -c <conn_name> -f ./SQL_Source_Code/snowsql_upload.sql -o output_file=/SQL_Source_Code/snowsql_upload.out
```

2. Upload data and jar files require for the demo either using PUT command or the snowpark setup jupyter notebook. Both options will yield same results. (or update and use the snowsql_upload.sql with snowsql client)
3. Load the raw data into different schema into RAW_PARQUET_TELCO table. This will help us simulate the data feed from S3 bucket through SNOWPIPE auto ingest
4. Setup a S3 bucket/folder using the storage integeration. This process is well documented [snowpipe setup here]
5. Start demo using the 01, 02, 03 .. worksheets
6. You will need a jupyter notebook environment for snowpark. Please follow the instructions if you don't already have one [in medium blog here]

<img src="/images/worksheets.png" />

[snowpipe setup here]: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3.html
[in medium blog here]: https://medium.com/snowflake/from-zero-to-snowpark-in-5-minutes-72c5f8ec0b55