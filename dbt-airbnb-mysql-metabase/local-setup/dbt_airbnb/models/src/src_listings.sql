WITH raw_listings AS (
        -- SELECT * FROM airbnb.raw_listings
        select * from {{ source('airbnb', 'listings') }}
)
SELECT
   id AS listing_id,
   listing_url,
   name AS listing_name,
   room_type,
   minimum_nights,
   host_id,
   price AS price_str,
   created_at,
   updated_at
FROM raw_listings