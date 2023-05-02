create procedure parse_and_validate_table_name(_in_table_name string, out _out_table_struct struct<dataset_name string, table_name string>)
options(
  strict_mode=false)
begin
  declare names_array default (select array_reverse(split(replace(_in_table_name, '`', ''), '.')));

  execute immediate """
    select (project_id||'.'||dataset_id, table_id)
    from """||
      ifnull('`'||names_array[safe_ordinal(3)]||'`.'||names_array[safe_ordinal(2)], ifnull('`'||@@project_id||'`.'||names_array[safe_ordinal(2)], '_SESSION'))||""".__TABLES__
    where table_id = '"""||names_array[safe_ordinal(1)]||"""'
  """ into _out_table_struct;

  execute immediate """
    assert @cond as
      'Table \""""||_in_table_name||"\" not found'"
  using _out_table_struct.table_name is not null as cond;
end
