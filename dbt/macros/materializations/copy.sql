{% materialization copy, adapter='bigquery' -%}

  {# Setup #}
  {{ run_hooks(pre_hooks) }}

  {% set copy_materialization = config.get('copy_materialization', default = 'table') %}
  {% set destination = this.incorporate(type='table') %}
  {% set incremental_existing_target = copy_materialization == 'incremental' and relation_exists(destination) %}

  {# ========== Create list of relations from model SQL ========== #}

  {% set ref_array = [] %}
  {% for ref_table in model.refs %}
    {{ ref_array.append(ref(ref_table.get('package'), ref_table.name, version=ref_table.get('version'))) }}
  {% endfor %}

  {% set src_array = [] %}
  {% for src_table in model.sources %}
    {{ src_array.append(source(*src_table)) }}
  {% endfor %}

  {# Create relations list starting from first relation in model SQL #}
  {% set relations_array = [] %}
  {% set relations_array = ref_array + src_array
      if 0 <= model.raw_code.find('ref(') <= model.raw_code.find('source(')
      else src_array + ref_array %}

  {# Check whether we have at least one realtion to work with #}
  {%- if relations_array | length == 0 -%}
    {{ exceptions.raise_compiler_error('No relation found in model SQL to copy data from') }}
  {%- endif -%}

  {# ========== Compatibility checks ========== #}

  {% set ns = namespace(single_copy=true) %}

  {# Determine relations comparison base #}
  {% set base_col_list = [] %}
  {% if incremental_existing_target %}
    {% for col in adapter.get_columns_in_relation(destination) %}
      {{ base_col_list.extend(col.flatten() | map('string') | list) }}
    {% endfor %}
    {% set comp_rel_index = 0 %}

    {% set first_col_list=[] %}
    {% for col in adapter.get_columns_in_relation(relations_array[0]) %}
      {{ first_col_list.extend(col.flatten() | map('string') | list) }}
    {% endfor %}
  {% else %}
    {% for col in adapter.get_columns_in_relation(relations_array[0]) %}
      {{ base_col_list.extend(col.flatten() | map('string') | list) }}
    {% endfor %}
    {% set comp_rel_index = 1 %}
  {% endif %}

  {# Check table structure for all relations #}
  {% for rel in relations_array[comp_rel_index:] %}
    {% set comp_col_list=[] %}
    {% for col in adapter.get_columns_in_relation(rel) %}
      {{ comp_col_list.extend(col.flatten() | map('string') | list) }}
    {% endfor %}

    {# Compare realtion structure with base and raise exception if it doesn't fit #}
    {% for col in comp_col_list %}
      {% if not col in base_col_list %}
        {{ exceptions.raise_compiler_error("Incompatible tables structure. " ~ col ~ " from  " ~ rel ~
          " doesn't match with " ~ (destination if incremental_existing_target else relations_array[0])) }}
      {%endif%}
    {% endfor %}

    {# Determine if possible to have single copy statement for all realtions #}
    {% if ns.single_copy and comp_col_list | sort != (first_col_list | sort if incremental_existing_target else base_col_list | sort) %}
      {% set ns.single_copy = false %}
    {% endif %}
  {% endfor %}

  {# ========== Working with database ========== #}

  {% if ns.single_copy %}
    {# Call adapter copy_table function to put information into destination using all relations as source #}
    {%- set result_str = adapter.copy_table(
        relations_array, destination, copy_materialization) -%}
    {{ store_result('main', response=result_str) }}
  {% else %}
    {# Create relation for interim table #}
    {%- set interim_dest = api.Relation.create(
        database=config.get('interim_database', default = this.database),
        schema=config.get('interim_schema', default = this.schema),
        identifier=this.name + '__dbt_tmp_' + local_md5(modules.datetime.datetime.now() | string)
    ) -%}

    {# Create interim table to put information there sequentially #}
    {%- call statement('main') %}
      create table {{ interim_dest }}
      like {{ destination if incremental_existing_target else relations_array[0] }};
    {%- endcall %}

    {# Call adapter copy_table function to append information into interim table sequentially for all relations #}
    {% for rel in relations_array %}
      {%- set result_str = adapter.copy_table(rel, interim_dest, 'incremental') -%}
      {{ store_result('main', response=result_str) }}
    {% endfor %}

    {# Call adapter copy_table function to put information into destination using interim table as source #}
    {%- set result_str = adapter.copy_table(
      interim_dest, destination, copy_materialization) -%}
    {{ store_result('main', response=result_str) }}

    {# Drop interim table #}
    {%- call statement('main') %}
      drop table if exists {{ interim_dest }};
    {%- endcall %}
  {% endif %}

  {# Clean up #}
  {{ run_hooks(post_hooks) }}
  {%- do apply_grants(target_relation, grant_config) -%}
  {{- adapter.commit() -}}

  {{ return({'relations': [destination]}) }}
{%- endmaterialization %}
