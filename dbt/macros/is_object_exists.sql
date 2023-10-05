{% macro is_object_exists(object) %}

  {%- set source_relation = adapter.get_relation(
    database=object.database,
    schema=object.schema,
    identifier=object.name)
  -%}

  {{ return(true if source_relation is not none else false) }}

{% endmacro %}
