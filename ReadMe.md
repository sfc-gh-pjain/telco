# Building pipeline for Customer Churn analysis

## Reference Architecture of the Demo
<img src="/images/ref_architecure.png" />

<b>
As a data engineering team member at a Telecom company, we have been tasked to build an end to end data pipeline in snowflake to support customer churn analysis by data science team. For this demo we have some customer data that our data science team would need.  We are responsible to build a feature store for the data science team. </b>


# Challenges
* As Data Engineers we want to build an end to end pipeline within snowflake and reduce the overall cost and technology footprints so that there are less point of failures
* On one hand we have customer billing and demographics data saved in semi structured format (PARQUET) and on other we have unstructured data in the form of emails
* We want to simplify the data pipeline by ingesting and processing the data closer within snowflake
But there are multiple developers persona in our team. Some know SQL, Some are Java and python professionals
* We also need to make data scientist’s life easier by cleaning, formatting, transforming and creating a feature store for them to refer in churn analysis

# Solution Overview

We are going to built an end to end data pipeline to built a BI, anaylytics data model and a feature store using new and old snowflake features. There are six features highted in this demo -

1. Snowpipe ingest, with latency improvements
2. Schema detection using INFER_SCHEMA
3. COPY HISTORY dashboard, new feature to visualize data loading with one click
4. Unstructured data support
5. Snowpark for Transformations and Data engineering
6. Serverless tasks. Update features store as an example

# Setup

Setup steps are documented in the [setup document here]

[setup document here]: https://github.com/sfc-gh-pjain/telco/blob/main/How_to_setup_demo.md