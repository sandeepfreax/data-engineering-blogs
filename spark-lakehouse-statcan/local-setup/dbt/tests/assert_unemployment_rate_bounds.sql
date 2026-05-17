-- Custom Singular Test: Unemployment rate must be between 0 and 100
--
-- This test FAILS (returns rows) if any unemployment rate is outside [0, 100].
-- dbt singular tests: a query that returns 0 rows = PASS, >0 rows = FAIL.
--
-- Why this matters:
--   Generic not_null and accepted_values tests cover nulls and categories.
--   But numeric bounds validation requires a custom singular test.
--   An unemployment rate of 150% or -5% is a data corruption indicator — it would silently flow into dashboards
--      without this guard.
--
-- Blog insight:
--   This is the kind of test that catches upstream StatCan format changes (e.g. if a future file ships rates as
--      decimals 0.0-1.0 instead of 0-100) before they corrupt your Gold tables.


select
    ref_date,
    geo,
    gender,
    age_group,
    value_thousands as unemployment_rate,
    'Unemployment rate out of bounds [0, 100]' as failure_reason
from {{ ref('fct_employment_monthly') }}
where
    labour_force_characteristic = 'Unemployment rate'
    and (
        value_thousands < 0
        or value_thousands > 100
    )