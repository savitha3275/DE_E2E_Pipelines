# Brazilian E-commerce End-to-End Data Pipeline (AWS S3 → Snowflake → dbt)

## Project Overview

This project implements an **end-to-end data engineering pipeline** for the Brazilian E-commerce dataset. The pipeline ingests raw CSV data into cloud storage, loads it into a data warehouse, and transforms it into analytics-ready tables using a modern data stack.

The architecture uses:

* Amazon S3 for raw data storage
* Snowflake for scalable data warehousing
* dbt for data transformations and modeling

The final output is a **structured analytics layer consisting of fact and dimension tables**.

---

# Architecture

```
CSV Files
   │
   ▼
AWS S3 (Data Lake)
   │
   ▼
Snowflake RAW Layer
   │
   ▼
dbt Staging Layer
   │
   ▼
dbt Intermediate Layer
   │
   ▼
dbt Mart Layer (Fact + Dimension Tables)
   │
   ▼
Analytics / BI
```

---

# 1. Data Ingestion

Raw CSV files containing Brazilian e-commerce data are uploaded to an **AWS S3 bucket**.

Example bucket:

```
brazilian-ecomm-dataset-a001
```

These files contain:

* customers
* orders
* products
* payments
* reviews
* sellers
* geolocation
* category translations

---

# 2. Snowflake Data Warehouse Setup

## Database and Schema

```sql
CREATE DATABASE BRAZIL_ECOMMERCE;
CREATE SCHEMA BRAZIL_ECOMMERCE.RAW;
```

Warehouse used:

```
COMPUTE_WH_TEST
```

Context setup:

```sql
USE WAREHOUSE COMPUTE_WH_TEST;
USE DATABASE BRAZIL_ECOMMERCE;
USE SCHEMA RAW;
```

---

# 3. Connecting Snowflake to AWS S3

A **Snowflake stage** is created to read files directly from S3.

```sql
CREATE STAGE olist_stage
URL='s3://brazilian-ecomm-dataset-a001'
CREDENTIALS=(AWS_KEY_ID='...' AWS_SECRET_KEY='...');
```

Check files:

```sql
LIST @olist_stage;
```

---

# 4. Loading Data from S3 into Snowflake

Raw tables are created in the **RAW schema**, and data is loaded using `COPY INTO`.

Example: Customers table

```sql
CREATE OR REPLACE TABLE raw_customers (
customer_id STRING,
customer_unique_id STRING,
customer_zip_code_prefix INTEGER,
customer_city STRING,
customer_state STRING
);
```

Load data:

```sql
COPY INTO raw_customers
FROM '@olist_stage/olist_customers_dataset.csv'
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"');
```

The following tables are created:

| Table                    | Description                   |
| ------------------------ | ----------------------------- |
| raw_customers            | customer data                 |
| raw_orders               | order details                 |
| raw_products             | product catalog               |
| raw_order_items          | items within orders           |
| raw_order_payments       | payment information           |
| raw_order_reviews        | customer reviews              |
| raw_sellers              | seller data                   |
| raw_geolocation          | location information          |
| raw_category_translation | product category translations |

---

# 5. dbt Project Setup

A dbt project is created to manage transformations.

Environment setup:

```
python -m venv venv
source venv/Scripts/activate
pip install dbt-core
pip install dbt-snowflake
```

Initialize project:

```
dbt init brazil_ecommerce
```

Verify connection:

```
dbt debug
```

---

# 6. dbt Project Structure

```
models/
│
├ staging/
│   src_customers.sql
│   src_orders.sql
│   src_products.sql
│   src_order_items.sql
│   src_payments.sql
│   src_reviews.sql
│   src_sellers.sql
│   src_geolocation.sql
│   src_category_translation.sql
│
├ intermediate/
│   int_orders_enriched.sql
│
├ dim/
│   dim_customers.sql
│   dim_products.sql
│   dim_sellers.sql
│   dim_product_category.sql
│
└ fct/
    fct_orders.sql
    fct_payments.sql
    fct_reviews.sql
```

---

# 7. Staging Layer

Each raw table has a corresponding **staging model**.

Purpose:

* clean raw data
* standardize column names
* prepare data for transformations

Example staging model:

```sql
WITH raw_customers AS (
SELECT *
FROM BRAZIL_ECOMMERCE.RAW.RAW_CUSTOMERS
)

SELECT
customer_id,
customer_unique_id,
customer_zip_code_prefix,
customer_city,
customer_state
FROM raw_customers
```

These models are materialized as **views**.

---

# 8. Intermediate Layer (Ephemeral Models)

Intermediate models perform transformations without creating physical tables.

Example:

```
int_orders_enriched
```

Materialization:

```
{{ config(materialized='ephemeral') }}
```

Purpose:

* join orders with payments
* enrich order data with payment values
* reduce unnecessary tables in the warehouse

Ephemeral models are compiled into **CTEs inside downstream queries**.

---

# 9. Dimension Tables

Dimension tables store **descriptive attributes** about business entities.

Rule used:

If the table answers:

```
Who / What / Where?
```

It is a **dimension table**.

Examples:

| Dimension            | Description                   |
| -------------------- | ----------------------------- |
| dim_customers        | customer attributes           |
| dim_products         | product attributes            |
| dim_sellers          | seller information            |
| dim_product_category | translated product categories |

Example:

```sql
SELECT
customer_id,
customer_city,
customer_state
FROM {{ ref('src_customers') }}
```

---

# 10. Fact Tables

Fact tables store **business events and metrics**.

Rule used:

If the table answers:

```
How many / How much / When?
```

It is a **fact table**.

Examples:

| Fact Table   | Description         |
| ------------ | ------------------- |
| fct_orders   | order transactions  |
| fct_payments | payment information |
| fct_reviews  | review metrics      |

Example:

```sql
SELECT
review_id,
order_id,
review_score,
review_creation_date
FROM {{ ref('src_reviews') }}
```

---

# 11. Seeds (Lookup Tables)

Seeds allow static CSV data to be loaded directly into the warehouse.

Seed file:

```
SEED_product_category_translation.csv
```

Load seed:

```
dbt seed
```

The seed creates:

```
SEED_product_category_translation
```

Purpose:

Translate Portuguese product categories to English for analytics.

Example query:

```sql
SELECT *
FROM BRAZIL_ECOMMERCE.RAW.SEED_product_category_translation
LIMIT 10;
```

---

# 12. Snapshot Implementation (SCD Type 2)

Snapshots track historical changes in data.

Snapshot example:

```sql
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
```

Snapshots automatically generate:

```
dbt_valid_from
dbt_valid_to
```

When a review changes:

* old record closes with `dbt_valid_to`
* new record is inserted with `dbt_valid_to = NULL`

---

# 13. Testing Snapshot Changes

Example change simulation:

```sql
UPDATE RAW_ORDER_REVIEWS
SET review_score = 5
WHERE review_id = '4a58847e0bc1838c7967a8d57bd408a9';
```

Run snapshot again:

```
dbt snapshot
```

Verify history:

```sql
SELECT review_id, review_score, dbt_valid_from, dbt_valid_to
FROM BRAZIL_ECOMMERCE.RAW_SNAPSHOTS.snap_reviews
ORDER BY dbt_valid_from;
```

---

# 14. Running dbt Commands

```
dbt seed
dbt run
dbt snapshot
dbt test


---

# 15. Final Data Warehouse Layers

```
RAW Layer
    │
    ▼
STAGING Layer (dbt views)
    │
    ▼
INTERMEDIATE Layer (ephemeral models)
    │
    ▼
MART Layer
   ├── Dimension Tables
   └── Fact Tables
```

---

# Conclusion

This project demonstrates a complete **modern data pipeline** using cloud technologies and the dbt transformation framework.

Key features implemented:

* Cloud data ingestion using S3
* Scalable warehouse using Snowflake
* Modular SQL transformations using dbt
* Star schema modeling (Fact + Dimension)
* Seeds for lookup tables
* Snapshots for historical tracking (SCD2)

The final dataset is optimized for **analytics and business intelligence reporting**.
