-- As of now, we have 2 issues that need to be fixed in the src_listings view:
-- 1. minimum_nights column has some records with value 0 which is not valid as per Airbnb data dictionary and we need to replace those values with 1
-- 2. price column is in string format and we need to convert it to decimal for further analysis

{{
	config(
		materialized = 'view'
    )
}}

with src_listings as (
    select *
    from {{ ref('src_listings') }}      -- this is view name created in src_listings.sql
)
SELECT
  listing_id,
  listing_name,
  room_type,
  CASE WHEN minimum_nights = 0 THEN 1 ELSE minimum_nights END AS minimum_nights,
  host_id,
  CAST(REPLACE(price_str, '$', '') AS DECIMAL(10, 2)) AS price,
  created_at,
  updated_at
FROM src_listings