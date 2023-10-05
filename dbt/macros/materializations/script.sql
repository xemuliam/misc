{%- materialization script, default %}

{{ run_hooks(pre_hooks) }}

{%- call statement('main') %}
{{ sql }}
{%- endcall %}

{{ run_hooks(post_hooks) }}

{{ return({'relations': []}) }}

{%- endmaterialization %}
