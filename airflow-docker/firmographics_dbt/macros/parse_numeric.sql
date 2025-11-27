{% macro parse_money_to_float(field) %}
    NULLIF(
        regexp_replace({{ field }}::string, '[$,]', ''),
        ''
    )::float
{% endmacro %}
