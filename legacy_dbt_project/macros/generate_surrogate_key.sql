-- generate_surrogate_key.sql
-- Macro: Generate MD5-based surrogate keys from composite columns
-- In Databricks migration, this is replaced by sha2() or monotonically_increasing_id()

{% macro generate_surrogate_key(field_list) %}
    MD5(
        {%- for field in field_list %}
            COALESCE(CAST({{ field }} AS VARCHAR), '_null_')
            {%- if not loop.last %} || '|' || {% endif -%}
        {%- endfor %}
    )
{% endmacro %}
