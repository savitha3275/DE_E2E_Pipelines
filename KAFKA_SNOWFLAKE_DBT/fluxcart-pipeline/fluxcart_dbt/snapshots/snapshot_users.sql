{% snapshot snapshot_users %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='USER_ID',
        strategy='check',
        check_cols=['USER_ID']
    )
}}

SELECT *
FROM {{ ref('dim_users') }}

{% endsnapshot %}