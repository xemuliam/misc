{% macro relation_exists(relation) %}
  {%- set source_relation = adapter.get_relation(
    database=relation.database,
    schema=relation.schema,
    identifier=relation.name)
  -%}
  {{ return(true if source_relation is not none else false) }}
{% endmacro %}