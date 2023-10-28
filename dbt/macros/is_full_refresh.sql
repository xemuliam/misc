{% macro is_full_refresh() %}

  {{ return(true if flags.FULL_REFRESH else false) }}

{% endmacro %}