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


- Implement dbt models for data cleansing and transformation
- Create dimensional models for analytics
- Set up automated pipeline orchestration (e.g., Airflow)
- Implement data quality checks and monitoring