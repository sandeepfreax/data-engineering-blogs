{% macro select_positive_values(model, column_name) %}
select * from {{ target.schema }}.{{ model }} where {{ column_name}} > 0
{% endmacro %}


-- can be compiled with inline expression as:
-- dbt compile --inline '{{ select_positive_values("dim_listings_cleansed", "minimum_nights") }}'

-- can be executed with inline expression as:
-- dbt show --inline '{{ select_positive_values("dim_listings_cleansed", "minimum_nights") }}'