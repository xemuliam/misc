create or replace procedure set_many_table_labels(
  _in_table_name STRING,
  _in_labels_array ARRAY<
    STRUCT<
      name STRING,
      value STRING
    >
  >
)
begin
  declare labels array<struct<name string, value string>> default [];

  call get_all_table_labels(_in_table_name, labels);

  set labels = (
    select array(
      select as struct l.*
      from unnest(labels) l
      where name not in (
        select name from unnest(_in_labels_array)
      )
    ) || _in_labels_array
  );

  execute immediate """
    alter table """||_in_table_name||"""
    SET OPTIONS (
      labels="""||(
        select '['||string_agg('STRUCT("'||l.name||'", "'||l.value||'")', ', ')||']'
        from unnest(labels) l
      )||"""
    )"""
  ;
end
