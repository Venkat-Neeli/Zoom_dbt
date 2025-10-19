{% macro data_quality_filter() %}
    record_status = 'ACTIVE' AND data_quality_score >= 0.7
{% endmacro %}
