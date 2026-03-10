/*
Fact model for reviews.
This model provides fact data for reviews from the staging reviews table.
*/
WITH reviews AS (

    SELECT * FROM {{ ref('src_reviews') }}

)

SELECT
    review_id,
    order_id,
    review_score,
    review_creation_date

FROM reviews