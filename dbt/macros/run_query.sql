{% macro run_query(sql, bulk=True) %}

  {%- if bulk -%}
    {%- for query in sql.split(';') -%}
      {%- if query | trim | length > 0 -%}
        {% do return(exec_statement(query)) %}
      {%- endif -%}
    {%- endfor -%}
  {%- else -%}
    {% do return(exec_statement(sql)) %}
  {%- endif -%}

{% endmacro %}

{% macro exec_statement(sql_statement) %}

  {% call statement("run_query_statement", fetch_result=true, auto_begin=false) %}
    {{ sql_statement }}
  {% endcall %}

  {% do return(load_result("run_query_statement").table) %}

{% endmacro %}
