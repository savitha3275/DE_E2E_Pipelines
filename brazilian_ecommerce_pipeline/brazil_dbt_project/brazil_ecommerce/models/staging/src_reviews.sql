/*
Staging model for order reviews.
This model selects review data from the raw order reviews table.
*/
WITH raw_reviews AS (

    SELECT *
    FROM BRAZIL_ECOMMERCE.RAW.RAW_ORDER_REVIEWS

)

SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp

FROM raw_reviews