{% macro calculate_data_quality_score(table_name, columns) %}
  CASE 
    {% for col in columns %}
    WHEN {{ col }} IS NULL THEN 0.0
    {% endfor %}
    ELSE 1.0
  END
{% endmacro %}
