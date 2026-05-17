-- Staging model: Employment by Industry (NAICS)
--
-- Same filter logic as stg_lfs_province.
-- Grain: one row per (ref_date, geo, naics_industry) after filtering to Seasonally adjusted Estimates only.


with source as (
    select * from silver_lfs_industry
),
filtered as (
    select
        ref_date,
        ref_year,
        ref_month,
        geo,
        dguid,
        naics_industry,
        data_type,
        scalar_factor,
        vector_id,
        value,
        is_suppressed,
        _ingested_at,
        _source_file
    from source
    where
        data_type = 'Seasonally adjusted'
        and statistic_type = 'Estimate'
        and is_suppressed = false
)
select * from filtered