{%- materialization script, default %}

  {# Setup #}
  {{ run_hooks(pre_hooks) }}

  {%- call statement('main') %}
    {{ sql }}
  {%- endcall %}

  {# Clean up #}
  {{ run_hooks(post_hooks) }}

  {{ return({'relations': []}) }}
{%- endmaterialization %}
