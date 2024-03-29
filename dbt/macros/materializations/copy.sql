{% materialization copy_new, adapter='bigquery' -%}

  {# Setup #}
  {{ run_hooks(pre_hooks) }}

  {% set copy_materialization = config.get('copy_materialization', default = 'table') %}
  {% set destination = this.incorporate(type='table') %}
  {% set incremental_existing_target = copy_materialization == 'incremental' and relation_exists(destination) %}

  {# ========== Create list of relations from model SQL ========== #}

  {# Create refs list #}
  {% set ref_array = [] %}
  {% for ref_table in model.refs %}
    {{ ref_array.append(ref(ref_table.get('package'), ref_table.name, version=ref_table.get('version'))) }}
  {% endfor %}

  {# Create sources list #}
  {% set src_array = [] %}
  {% for src_table in model.sources %}
    {{ src_array.append(source(*src_table)) }}
  {% endfor %}

  {# Create all relations list starting from first relation in model SQL #}
  {% set relations_array = [] %}
  {% set relations_array = ref_array + src_array
      if 0 <= model.raw_code.find('ref(') <= model.raw_code.find('source(')
      else src_array + ref_array %}

  {# Check whether we have at least one realtion to work with #}
  {%- if relations_array | length == 0 -%}
    {{ exceptions.raise_compiler_error('No relation found in model SQL to copy data from') }}
  {%- endif -%}

  {# ========== Compatibility checks ========== #}

  {% set ns = namespace(is_matched_with_any_group=false) %}

  {# Determine relations groups list. Each group will be performed as single copy job #}
  {% set rel_grp_list=[[]] %}
  {{ rel_grp_list[0].extend([relations_array[0]]) }}

  {# Determine columns groups list. Contains unique columns lists across all model relations #}
  {% set col_grp_list=[[]] %}
  {% for col in adapter.get_columns_in_relation(relations_array[0]) %}
    {{ col_grp_list[0].extend(col.flatten() | map('string') | list) }}
  {% endfor %}

  {# Determine columns list for target table #}
  {% set target_col_list = [] %}
  {% for col in adapter.get_columns_in_relation(destination if incremental_existing_target else relations_array[0]) %}
    {{ target_col_list.extend(col.flatten() | map('string') | list) }}
  {% endfor %}

  {# Loop over all realstions to made checks #}
  {% for rel in relations_array[0 if incremental_existing_target else 1:] %}
    {# Determine flattened (to cover nested columns) table structure from relation need to be checked #}
    {% set comp_col_list=[] %}
    {% for col in adapter.get_columns_in_relation(rel) %}
      {{ comp_col_list.extend(col.flatten() | map('string') | list) }}
    {% endfor %}

    {# Compare realtion structure with target and raise exception if it doesn't fit #}
    {% for col in comp_col_list %}
      {% if not col in target_col_list %}
        {{ exceptions.raise_compiler_error("Incompatible tables structure. " ~ col ~ " from  " ~ rel ~
          " doesn't match with " ~ (destination if incremental_existing_target else relations_array[0])) }}
      {%endif%}
    {% endfor %}

    [# Maintain relations groups  #]
    {% set rel_index =  loop.index - (1 if incremental_existing_target else 0) %}
    
    {# Skip first realtion because it's alredy added into relations and columns groups #}
    {% if rel_index > 0 %}
      {% set ns.is_matched_with_any_group = false %}

      {# Compare relation's structure with each element in unique column groups #}
      {% for col_grp in col_grp_list %}
        {# Check if relation structure exists in a group #}
        {% if comp_col_list | sort == col_grp | sort %}
          {# Check if realtion doen't exist in relation group and add into group or raise exception#}
          {% if not relations_array[rel_index] in rel_grp_list[loop.index - 1] %}
            {{ rel_grp_list[loop.index - 1].extend([relations_array[rel_index]]) }}
            {% set ns.is_matched_with_any_group = true %}
          {% else %}
            {{ exceptions.raise_compiler_error("Relation " ~ relations_array[rel_index] ~ 
              " already exists in the model. Please revise model SQL") }}
          {% endif %}
        {% endif %}
      {% endfor %}

      {# Maintain new column group and relation group if relation stucture doesn't match with existing groups  #}
      {% if not ns.is_matched_with_any_group %}
        {{ col_grp_list.append(comp_col_list) }}
        {{ rel_grp_list.append([relations_array[rel_index]]) }}
      {% endif %}
    {% endif %}
  {% endfor %}

  {# ========== Working with database ========== #}

  {# Determine whether we can use singlr copy statement for all relations #}
  {% set single_copy = true if rel_grp_list | length == 1 else false %}

  {% if not single_copy %}
    {# Create relation for interim table to put information there sequentially #}
    {%- set interim_dest = api.Relation.create(
        database=config.get('interim_database', default = this.database),
        schema=config.get('interim_schema', default = this.schema),
        identifier=this.name + '__dbt_tmp_' + local_md5(modules.datetime.datetime.now() | string)
    ) -%}

    {# Create empty interim table using appropriate table tructure and metadata #}
    {%- call statement('main') %}
      create table {{ interim_dest }}
      like {{ destination if incremental_existing_target else relations_array[0] }};
    {%- endcall %}
  {% endif %}

  {# Call adapter copy_table function for each relation group #}
  {% for rel_grp in rel_grp_list %}
    {%- set result_str = adapter.copy_table(rel_grp, destination if single_copy else interim_dest,
      copy_materialization if single_copy else 'incremental') -%}
    {{ store_result('main', response=result_str) }}
  {% endfor %}

  {% if not single_copy %}
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
