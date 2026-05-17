-- Dimension: Geography
--
-- PURPOSE:
--   A simple geography dimension derived from Silver data.
--   Provides stable surrogate keys and metadata for all province/territory rows.
--
-- DESIGN NOTES:
--   We use dbt_utils.generate_surrogate_key() for the surrogate key.
--   This produces a deterministic MD5 hash of the natural key (dguid + geo) so the key is stable across full-refresh
--      runs — no identity column needed.
--
--   DGUID is Statistics Canada's official geographic unique identifier.
--   It is more stable than the GEO name string (which can have trailing spaces or encoding differences between
--      StatCan releases).


{{
  config(
    materialized='table',
    file_format='delta',
    tags=['gold']
  )
}}

with geo_base as (
    select distinct
        geo,
        dguid
    from {{ ref('stg_lfs_province') }}
),
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['dguid', 'geo']) }} as geo_key,
        geo as geo_name,
        dguid,
        -- Classify Canada-level vs province-level for easier filtering
        case
            when geo = 'Canada' then 'National'
            else 'Province'
        end as geo_level,
        -- Region grouping — useful for regional aggregation in Gold marts
        case
            when geo in ('British Columbia', 'Alberta', 'Saskatchewan', 'Manitoba') then 'Western Canada'
            when geo in ('Ontario', 'Quebec') then 'Central Canada'
            when geo in ('New Brunswick', 'Nova Scotia', 'Prince Edward Island', 'Newfoundland and Labrador') then 'Atlantic Canada'
            when geo = 'Canada' then 'National'
            else 'Other'
        end as geo_region
    from geo_base
)
select * from final