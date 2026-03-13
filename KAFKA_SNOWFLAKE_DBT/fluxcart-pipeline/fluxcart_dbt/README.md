Welcome to your new dbt project!

### Using the starter project

Kafka Streams
      ↓
Python Consumers
      ↓
Snowflake RAW
      ↓
dbt Staging
      ↓
dbt Fact Models
      ↓
dbt Dimension Models
      ↓
dbt Snapshots
      ↓
dbt Tests
      ↓
dbt Docs

Try running the following commands:
- dbt run
- dbt test


### Resources:
Incremental: only new rows are merged

unique_key ensures no duplicates

ref('stg_analytics') → dbt knows the dependency

Same pattern applies for fct_fraud and fct_inventory.
For fct_fraud, use alert_id as unique_key.
For fct_inventory, use order_events + event_ts composite as unique_key.
