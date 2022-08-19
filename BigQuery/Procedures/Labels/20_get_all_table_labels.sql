create or replace procedure get_all_table_labels(
  _in_table_name string,
  
  out _out_labels_array array<
    struct<
      name string,
      value string
    >
  >
)

begin
  declare table_info struct<dataset_name string, table_name string>;
  declare get_labels default '';

  call parse_table_name(_in_table_name, table_info);

  execute immediate """
    select ifnull(any_value(option_value), '')
    from `"""||table_info.dataset_name||""".INFORMATION_SCHEMA.TABLE_OPTIONS`
    where table_name = '"""||table_info.table_name||"""'
      and option_name = 'labels'
  """ into get_labels;

  if length(get_labels) > 0 then
    execute immediate "select " || get_labels into _out_labels_array;
  else
    set _out_labels_array = [];
  end if;
end
