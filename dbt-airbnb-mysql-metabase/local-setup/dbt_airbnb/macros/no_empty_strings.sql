{% macro no_empty_strings(model) %}
    {%- for col in adapter.get_columns_in_relation(model) -%}
        {%- if col.is_string() %}
            {{ col.name }} IS NOT NULL AND {{ col.name }} <> '' AND
        {%- endif %}
    {%- endfor %}
    TRUE
{% endmacro %}

-- can be compile inline as
-- dbt compile --inline 'select * from {{ ref("dim_hosts_cleansed") }} where {{ no_empty_strings(ref("dim_hosts_cleansed")) }}'

-- can be executed inline as
-- dbt show --inline 'select * from {{ ref("dim_hosts_cleansed") }} where {{ no_empty_strings(ref("dim_hosts_cleansed")) }}'