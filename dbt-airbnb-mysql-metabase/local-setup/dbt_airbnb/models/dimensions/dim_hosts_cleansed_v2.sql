-- This model cleanses the `src_hosts` source by replacing NULL values in the `host_name` column with 'Anonymous'.

{{
	config(
		materialized = 'table'
    )
}}


WITH src_hosts AS (
    SELECT *
    FROM {{ ref('src_hosts') }}
)
SELECT
    host_id,
    IFNULL(host_name, 'N/A') AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM src_hosts