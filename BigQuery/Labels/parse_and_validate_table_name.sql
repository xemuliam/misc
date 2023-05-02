create procedure parse_and_validate_table_name(_in_table_name string, out _out_table_struct struct<dataset_name string, table_name string>)
options(
  strict_mode=false)
begin
  declare names_array default (select array_reverse(split(replace(_in_table_name, '`', ''), '.')));
  declare is_valid default false;

  execute immediate """"""
    select exists(
      select 1
      from `""""""||ifnull(names_array[safe_ordinal(3)], @@project_id)||'.'||names_array[safe_ordinal(2)]||"""""".INFORMATION_SCHEMA.TABLES`
      where table_name = '""""""||names_array[safe_ordinal(1)]||""""""'
    )
  """""" into is_valid;

  execute immediate """"""
    assert @cond as
      'table \""""""""||_in_table_name||""\"" not found'""
  using is_valid as cond;

  set _out_table_struct = (ifnull(names_array[safe_ordinal(3)], @@project_id)||'.'||names_array[safe_ordinal(2)], names_array[safe_ordinal(1)]);
end
