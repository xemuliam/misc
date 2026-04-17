{% macro sleep(seconds) %}

  {% set sleep_query %}
    declare cutoff_time default timestamp_add(current_timestamp, interval {{ seconds }} second);
    declare should_wait default true;

    while should_wait do
      set should_wait = (current_timestamp < cutoff_time);
    end while;
  {% endset %}

  {% do run_query(sleep_query) %}

  {% if execute %}
     {{ log("Slept for " ~ seconds ~ " seconds", info=True) }}
  {% endif %}

{% endmacro %}
