{% macro calculate_data_quality_score(null_checks, format_checks, range_checks) %}
    CASE 
        WHEN ({{ null_checks }}) AND ({{ format_checks }}) AND ({{ range_checks }}) THEN 1.00
        WHEN ({{ null_checks }}) AND ({{ format_checks }}) THEN 0.80
        WHEN ({{ null_checks }}) AND ({{ range_checks }}) THEN 0.70
        WHEN ({{ null_checks }}) THEN 0.60
        ELSE 0.00
    END
{% endmacro %}
