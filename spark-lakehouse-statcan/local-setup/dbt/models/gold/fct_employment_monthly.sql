-- Fact: Monthly Employment Summary
--
-- PURPOSE:
--   Monthly employment metrics by province and gender.
--   This is the primary Gold table for dashboards and trend analysis.
--
-- GRAIN:
--   One row per (ref_date, geo, gender, age_group, labour_force_characteristic) for the 'Total - Gender' and both
--      gender splits, 'All ages' cohort.
--   This matches what most employment dashboards show — headline numbers.
--
-- METRICS INCLUDED:
--   value_thousands — raw value from StatCan (persons in thousands)
--   value_actual — value_thousands * 1000 (for display convenience)
--
-- JOIN TO INDUSTRY:
--   We join the industry-level total employment to province-level totals using ref_date + geo as the bridge key. This
--      produces a fact table that can answer both "employment by characteristic" and "employment by industry" questions
--      from a single Gold table.
--
-- BLOG INSIGHT:
--   We do NOT join on every NAICS industry — only the national total ("Total employed, all industries") to provide a
--      cross-check column.
--   Joining all 30+ NAICS codes here would create a wide, hard-to-use table.
--   Industry breakdowns belong in a separate fact table (fct_industry_monthly).


{{
  config(
    materialized='table',
    file_format='delta',
    tags=['gold']
  )
}}


with province as (
    select
        ref_date,
        ref_year,
        ref_month,
        geo,
        labour_force_characteristic,
        gender,
        age_group,
        unit_of_measure,
        scalar_factor,
        value as value_thousands,
        round(value * 1000, 0) as value_actual,
        data_type,
        statistic_type,
        _ingested_at,
        _source_file
    from {{ ref('stg_lfs_province') }}
    -- Focus on total age group for headline metrics
    where age_group = '15 years and over'
),
industry_totals as (
    -- National total employment from industry table — for cross-check only
    select
        ref_date,
        geo,
        value as industry_total_thousands
    from {{ ref('stg_lfs_industry') }}
    where
        naics_industry = 'Total employed, all industries'
        and geo != 'Canada'
),
final as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['p.ref_date', 'p.geo', 'p.labour_force_characteristic', 'p.gender', 'p.age_group']) }} as fact_key,
        -- Time
        p.ref_date,
        p.ref_year,
        p.ref_month,
        -- Dimensions
        p.geo,
        p.labour_force_characteristic,
        p.gender,
        p.age_group,
        -- Measures
        p.value_thousands,
        p.value_actual,
        p.data_type,
        p.statistic_type,
        p.unit_of_measure,
        p.scalar_factor,
        -- Cross-check from industry table (null if no match)
        i.industry_total_thousands,
        -- Audit
        p._ingested_at,
        p._source_file
    from province p
    left join industry_totals i
        on  p.ref_date  = i.ref_date
        and p.geo = i.geo
)
select * from final