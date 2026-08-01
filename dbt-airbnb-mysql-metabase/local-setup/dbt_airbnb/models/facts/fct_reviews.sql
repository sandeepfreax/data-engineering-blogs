-- We can specify materialization at .sql level too

{{
    config(
        materialized = 'incremental',
        on_schema_change = 'fail',
        event_time='review_date'
    )
}}

with src_reviews as (
  select * from {{ ref('src_reviews') }}
)
select
*
from src_reviews
where review_text is not null
-- since this is an incremental load, we need to specify the condition to load only new records.
-- for this, we will use review_date as the incremental key and will load only records which are greater than the max review_date in the current table.
{% if is_incremental() %}
  and review_date > (select max(review_date) from {{ this }})
{% endif %}
-- we can further enhance this functionally using macro variables, where we can explicitly pass some variable name
-- and use them here for filtering