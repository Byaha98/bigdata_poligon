{% macro moscow_time_macro(column_name) %}
    toTimezone({{ column_name }}, 'Europe/Moscow')
{% endmacro %}
