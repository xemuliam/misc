create or replace procedure set_many_table_labels(p_table_name STRING, p_labels_array ARRAY<STRUCT<name STRING, value STRING>>)
begin
  declare labels array<struct<name string, value string>> default [];

  call sdp_internal.get_all_table_labels(p_table_name, labels);

  set labels = (
    select array(
      select as struct l.*
      from unnest(labels) l
      where name not in (
        select name
        from unnest(p_labels_array)
      )
    ) || p_labels_array
  );

  execute immediate """
    alter table """||p_table_name||"""
    SET OPTIONS (
      labels=""" || (
        select '[' || string_agg('STRUCT("'||l.name||'", "'||l.value||'")', ', ') || ']'
        from unnest(labels) l
      ) || """
    )"""
  ;
end
