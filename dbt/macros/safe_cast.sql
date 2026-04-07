{% macro safe_cast_float(expression) %}
    CAST({{ expression }} AS DECIMAL(18, 6))
{% endmacro %}

{% macro safe_cast_int(expression) %}
    CAST({{ expression }} AS BIGINT)
{% endmacro %}

{% macro safe_cast_date(expression, format='YYYY-MM-DD') %}
    TO_DATE({{ expression }}, '{{ format }}')
{% endmacro %}

{% macro safe_cast_timestamp(expression) %}
    CAST({{ expression }} AS TIMESTAMPTZ)
{% endmacro %}

{% macro safe_divide(numerator, denominator, default=0) %}
    CASE
        WHEN ({{ denominator }}) = 0 OR ({{ denominator }}) IS NULL
        THEN {{ default }}
        ELSE ({{ numerator }})::DECIMAL / ({{ denominator }})
    END
{% endmacro %}
