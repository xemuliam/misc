{% macro is_full_refresh() %}
  {{ return(true if config.get('full_refresh') or flags.FULL_REFRESH else false) }}
{% endmacro %}