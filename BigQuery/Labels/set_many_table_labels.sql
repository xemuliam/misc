create procedure set_many_table_labels(_in_table_name string, _in_labels_array array<struct<name string, value string>>)
options(
  strict_mode=false)
begin
  declare labels array<struct<name string, value string>> default [];
  declare table_struct STRUCT<dataset_name STRING, table_name STRING> default null;

  call get_all_table_labels(_in_table_name, labels);

  set labels = (
    select array(
      select as struct l.*
      from unnest(labels) l
      where name not in (
        select name
        from unnest(_in_labels_array)
      )
    )||_in_labels_array
  );

  call parse_table_name(_in_table_name, table_struct);
  execute immediate """
    alter table """||table_struct.dataset_name||'.'||table_struct.table_name||"""
    SET OPTIONS (
      labels="""||(
        select '['||ifnull(string_agg('STRUCT("'||l.name||'", "'||l.value||'")', ', '), '')||']'
        from unnest(labels) l
      )||"""
    )""";
end
