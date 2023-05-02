create procedure parse_table_name(_in_table_name string, out _out_table_struct struct<dataset_name string, table_name string>)
options(
  strict_mode=false)
begin
  declare dataset_name, table_name string default null;
  declare names_array default (select array_reverse(split(replace(_in_table_name, '`', ''), '.')));

  set table_name = nullif(names_array[safe_ordinal(1)], '');
  set dataset_name = ifnull(names_array[safe_ordinal(3)], @@project_id)||'.'||nullif(names_array[safe_ordinal(2)], '');

  assert (table_name is not null and dataset_name is not null)
    as 'Dataset name or table name is missing';

  set _out_table_struct = (dataset_name, table_name);
end
