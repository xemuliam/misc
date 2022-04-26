create or replace procedure parse_table_name(p_table_name STRING, OUT p_table_struct STRUCT<dataset_name STRING, table_name STRING>)
begin
  declare dataset_name, table_name string default null;
  declare names_array default (select array_reverse(split(replace(p_table_name, '`', ''), '.')));

  set table_name = nullif(names_array[safe_ordinal(1)], '');
  set dataset_name = ifnull(nullif(names_array[safe_ordinal(3)], '')||'.', '') || nullif(names_array[safe_ordinal(2)], '');

  assert (table_name is not null and dataset_name is not null)
    as 'Dataset or table name is missing';

  set p_table_struct = (dataset_name, table_name);
end
