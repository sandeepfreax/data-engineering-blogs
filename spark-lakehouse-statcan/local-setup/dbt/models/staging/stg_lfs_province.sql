-- Staging model: Labour Force Characteristics by Province
--
-- PURPOSE:
--   Thin, opinionated view over the Silver lfs_province Delta table.
--   Staging models do three things and ONLY three things:
--     1. Rename columns to dbt project conventions (already done in Silver)
--     2. Filter to the rows this project cares about
--     3. Cast any remaining type adjustments
--   NO business logic. NO aggregations. NO joins.
--
-- FILTER DECISIONS:
--   data_type = 'Seasonally adjusted':
--     StatCan publishes three data types: Seasonally adjusted, Unadjusted, Trend-cycle. For employment analysis,
--      seasonally adjusted is the standard — it removes calendar effects (holidays, weather) so month-over-month
--      comparisons are meaningful.
--     Blog insight: choosing the wrong data_type silently invalidates all downstream trend analysis.
--   statistic_type = 'Estimate':
--     StatCan also publishes standard errors and confidence intervals.
--     We keep only the point estimate here. Standard errors can be added as a separate staging model if needed for
--      uncertainty analysis.
--   is_suppressed = false:
--     We exclude suppressed rows from Gold aggregations to avoid fabricating values. Suppression analysis (which
--      regions/periods have the most suppression) is a separate analytical question.


with source as (

    select * from silver_lfs_province

),

filtered as (

    select
        ref_date,
        ref_year,
        ref_month,
        geo,
        dguid,
        labour_force_characteristic,
        gender,
        age_group,
        data_type,
        unit_of_measure,
        scalar_factor,
        vector_id,
        value,
        is_suppressed,
        data_type,
        statistic_type,
        _ingested_at,
        _source_file

    from source

    where
        data_type     = 'Seasonally adjusted'
        and statistic_type  = 'Estimate'
        and is_suppressed   = false

)

select * from filtered