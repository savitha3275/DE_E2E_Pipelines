# Brazilian E-commerce End-to-End Pipeline (AWS, Snowflake, dbt)

This project implements an end-to-end data pipeline for Brazilian e-commerce data, utilizing AWS S3 for data storage, Snowflake for data warehousing, and dbt for data transformation.

## Overview

The pipeline processes raw e-commerce data from CSV files, loads it into AWS S3, and then ingests it into Snowflake for further analysis and transformation using dbt.

## Data Pipeline Steps

### 1. Data Loading to S3
- Raw CSV files are uploaded to AWS S3 bucket
- Files contain Brazilian e-commerce data including customers, orders, products, etc.

### 2. Snowflake Setup
- **Database**: `BRAZIL_ECOMMERCE`
- **Schema**: `RAW`
- **Warehouse**: `COMPUTE_WH_TEST`

### 3. dbt Configuration
- dbt namespace altered to `BRAZIL_ECOMMERCE.RAW`
- Ensures proper targeting of the raw schema for data transformations

### 4. Stage Creation
- Created `olist_stage` for both S3 and Snowflake integration
- Enables seamless data loading from S3 into Snowflake tables

### 5. Snowflake Context Setup
```sql
USE WAREHOUSE COMPUTE_WH_TEST;
USE DATABASE BRAZIL_ECOMMERCE;
USE SCHEMA RAW;
```

### 6. Raw Tables Creation and Data Ingestion

The following raw tables were created in the `RAW` schema and populated with data from corresponding CSV files:

- **RAW_CATEGORY_TRANSLATION**: Product category translations
- **RAW_CUSTOMERS**: Customer information
- **RAW_GEOLOCATION**: Geographic location data
- **RAW_ORDERS**: Order details
- **RAW_ORDER_ITEMS**: Individual items within orders
- **RAW_ORDER_PAYMENTS**: Payment information for orders
- **RAW_ORDER_REVIEWS**: Customer reviews for orders
- **RAW_PRODUCTS**: Product catalog information
- **RAW_SELLERS**: Seller information

## Architecture

```
CSV Files → AWS S3 → Snowflake RAW Schema → dbt Transformations → Analytics
```

## Prerequisites

- AWS account with S3 access
- Snowflake account with appropriate permissions
- dbt installed and configured
- Access to the raw CSV data files

## Usage

1. Upload CSV files to S3 bucket --> Click on s3 --> Click on create bucket
My bucket name : brazilian-ecomm-dataset-a001 --> Click on create bucket -->Click on upload
Click on add files
2. Execute Snowflake setup scripts to create database, schema, and stages
3. LOAD DATA FROM AWS S3 INTO SNOWFLAKE. Now for s3 to snowflake connection, we need stages
4. Now go to vscode. Open gitbash terminal:

mkdir brazil_dbt_project
cd brazil_dbt_project
 
Python -m venv venv
source venv/Scripts/activate

pip install dbt-core
pip install dbt-snowflake

mkdir %userprofile%\.dbt

Go to snowflake and take acc identifier detail.

5. Run dbt models to transform raw data into analytics-ready tables. --> dbt init brazil_ecommerce --> cd brazil_ecommerce
dbt debug
cd models
Mkdir staging dim fct
Each RAW table → one staging model.
Example: customers.

Update yml file --> 
version: 2
sources:
  - name: raw
    database: BRAZIL_ECOMMERCE
    schema: RAW
    tables:
      - name: raw_customers
      - name: raw_orders
      - name: raw_products
      - name: raw_order_items
      - name: raw_payments
      - name: raw_reviews
      - name: raw_sellers
      - name: raw_geolocation
      - name: raw_category_translation
  
  Now all 9 models from staging are added as views in snowflake. Which has raw data in it.

1. Query transformed data for business insights

## Next Steps

Since your staging views are already in the RAW schema, the next step is to build the transformation layers in DEV and demonstrate dbt features (materializations, seeds, snapshots, ephemeral).

SEED implementation: Seeds are small static datasets stored as CSV files in dbt. They are commonly used for lookup tables like category translations and then joined to build dimension or fact models.

dbt will create a table named:

SEED_product_category_translation

inside your warehouse schema.

You can reference it in models using:

{{ ref('SEED_product_category_translation') }}

STEP1:

Create:

models/dim_product_category.sql
SELECT
product_category_name AS category_name_pt,
product_category_name_english AS category_name_en
FROM {{ ref('product_category_name_translation') }}

Why this dimension exists:
analysts want English category names
dashboards become readable


STEP 2 :
Fact table joining the seed

Now join it with products.

Create:

models/fct_products.sql

SELECT
p.product_id,
p.product_category_name,
c.product_category_name_english AS category_name_en
FROM {{ ref('stg_products') }} p
LEFT JOIN {{ ref('product_category_name_translation') }} c
ON p.product_category_name = c.product_category_name


Action	            Command
Load CSV	        dbt seed
Build models	    dbt run
Check results	    SQL in Snowflake

to verify seed worked :
SELECT
product_id,
product_category_name,
category_name_en
FROM BRAZIL_ECOMMERCE.raw_dev.fct_products
LIMIT 20;

Ephemeral implementation- will not have any table or view created but uses CTEs.
{{ config(materialized='ephemeral') }}

Example : intermediate model.

Snapshot implementation using SCD-2:

/*
Snapshot model for reviews.
This snapshot captures changes in review scores and comments over time.
*/
{% snapshot snap_reviews %}

{{
config(
target_schema='RAW_SNAPSHOTS',
unique_key='review_id',
strategy='check',
check_cols=['review_score','review_comment_message']
)
}}

SELECT *
FROM {{ ref('src_reviews') }}

{% endsnapshot %}

In snapshot, old version will be closed with dbt_valid_to date and a new version created with 'null' in dbt_valid_to column