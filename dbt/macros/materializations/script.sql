{%- materialization script, default %}

  {# Setup #}
  {{ run_hooks(pre_hooks) }}
  
  {%- if config.get('bulk', default=False) -%}
    {%- for query in sql.split(';') -%}
      {%- if query | trim | length > 0 -%}
        {% call statement('main') %}
          {{ query }}
        {% endcall %}
      {%- endif -%}
    {%- endfor -%}
  {%- else -%}
    {% call statement('main') %}
      {{ sql }}
    {% endcall %}
  {%- endif -%}
  
  {# Clean up #}
  {{ run_hooks(post_hooks) }}

  {{ return({'relations': []}) }}

{%- endmaterialization %}
