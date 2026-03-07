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