# FluxCart Real-Time Data Pipeline

Kafka → Snowflake → dbt Analytics Platform


Streaming ingestion (Kafka)
Warehouse storage (Snowflake)
Transformation (dbt)
Data modeling (star schema)
History tracking (snapshots)
Data quality (tests)
Documentation (dbt docs)

## Project Overview

This project demonstrates a **modern end-to-end real-time data pipeline** built for an e-commerce platform (FluxCart).
The system streams event data through Kafka, loads it into Snowflake, and transforms it using dbt to create analytics-ready data models.

The pipeline simulates real-world event streams such as:

* User behavior events
* Order transactions
* Fraud alerts
* Inventory updates

The final result is a **fully modeled data warehouse with fact tables, dimension tables, snapshots, tests, and documentation**.

---

## Architecture

```
Kafka Producers
      │
      ▼
Kafka Topics
(behavior, orders, fraud, inventory)
      │
      ▼
Python Consumers
      │
      ▼
Snowflake RAW Tables
      │
      ▼
dbt Staging Models (Cleaning & Standardization)
      │
      ▼
dbt Fact Tables (Incremental)
      │
      ▼
dbt Dimension Tables
      │
      ▼
dbt Snapshots (SCD Type 2)
      │
      ▼
Analytics-Ready Warehouse
```

---

## Tech Stack

| Layer           | Technology   |
| --------------- | ------------ |
| Streaming       | Kafka        |
| Data Processing | Python       |
| Data Warehouse  | Snowflake    |
| Transformation  | dbt          |
| Infrastructure  | Docker       |
| Language        | SQL + Python |

---

## Repository Structure

```
fluxcart-pipeline
│
├── kafka
│   ├── producer.py
│   ├── fraud_consumer.py
│   ├── analytics_consumer.py
│   └── inventory_consumer.py
│
├── docker
│   └── docker-compose.yml
│
├── fluxcart_dbt
│   ├── models
│   │   ├── staging
│   │   ├── marts
│   │   └── dims
│   │
│   ├── snapshots
│   ├── seeds
│   └── schema.yml
│
└── README.md
```

---

## Data Warehouse Design

### Fact Tables

| Table           | Description                 |
| --------------- | --------------------------- |
| `FCT_ANALYTICS` | Aggregated behavior metrics |
| `FCT_FRAUD`     | Fraud detection alerts      |
| `FCT_INVENTORY` | Inventory events            |

---

### Dimension Tables

| Table            | Description           |
| ---------------- | --------------------- |
| `DIM_USERS`      | Unique users          |
| `DIM_PRODUCTS`   | Product catalog       |
| `DIM_WAREHOUSES` | Warehouse information |

---

## dbt Features Implemented

This project demonstrates multiple dbt capabilities used in production environments.

### Staging Models

Used for cleaning and standardizing raw data.

```
models/staging
```

Materialization:

```
view
```

---

### Incremental Fact Tables

Fact tables are built incrementally to process only new data.

```
models/marts
```

Configuration example:

```yaml
+materialized: incremental
+incremental_strategy: merge
```

---

### Dimension Models

Dimension tables store reference data used for analytics joins.

```
models/dims
```

---

### Snapshots (Slowly Changing Dimensions)

User history is tracked using **dbt snapshots (SCD Type 2)**.

```
snapshots/snapshot_users.sql
```

---

### Data Tests

Data quality tests are implemented using dbt.

Examples:

* Not null checks
* Unique constraints

Run tests:

```
dbt test
```

---

### Documentation

dbt automatically generates documentation and lineage graphs.

Generate docs:

```
dbt docs generate
```

Start documentation server:

```
dbt docs serve
```

Open:

```
http://localhost:8080
```

---

## Running the Pipeline

### 1 Start Kafka Infrastructure

```
docker compose up -d
```

---

### 2 Create Kafka Topics

```
python setup_topics.py
```

---

### 3 Start Data Producers

```
python producer.py
```

---

### 4 Start Kafka Consumers

```
python analytics_consumer.py
python fraud_consumer.py
python inventory_consumer.py
```

These consumers load streaming data into **Snowflake RAW tables**.

---

### 5 Run dbt Transformations

```
cd fluxcart_dbt
dbt run
```

---

### 6 Run Data Tests

```
dbt test
```

---

### 7 Run Snapshots

```
dbt snapshot
```

---

### 8 Generate Documentation

```
dbt docs generate
dbt docs serve
```

---

## Example Analytics Queries

-- analytical queries
-- Top 5 products by revenue
SELECT TOP_PRODUCT, SUM(TOTAL_REVENUE) AS revenue
FROM FCT_ANALYTICS
GROUP BY TOP_PRODUCT
ORDER BY revenue DESC;

--Warehouse fulfillment rate (handle division by zero)
SELECT 
    TOP_WAREHOUSE, 
    SUM(ORDERS_DELIVERED) / NULLIF(SUM(ORDERS_PLACED),0) * 100 AS fulfillment_rate
FROM FCT_INVENTORY
GROUP BY TOP_WAREHOUSE
ORDER BY fulfillment_rate DESC;

-- Fraud alerts summary by severity

SELECT 
    SEVERITY, 
    COUNT(*) AS alert_count
FROM FCT_FRAUD
GROUP BY SEVERITY
ORDER BY alert_count DESC;

---

## Key Concepts Demonstrated

* Real-time streaming pipelines
* Event-driven data ingestion
* Modern data warehouse modeling
* Incremental data processing
* Slowly Changing Dimensions
* Data quality testing
* Data lineage and documentation

---

## Future Improvements

Possible enhancements include:

* Airflow orchestration
* Kafka schema registry
* Real-time dashboards
* CI/CD for dbt pipelines
* Data observability tools

---

## Author

Savithadevi Adikesavan
Data Engineering Portfolio Project
