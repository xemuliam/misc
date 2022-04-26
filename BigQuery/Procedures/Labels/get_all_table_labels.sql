create or replace procedure get_all_table_labels(p_table_name STRING, OUT p_labels_array ARRAY<STRUCT<name STRING, value STRING>>)
begin
  declare table_info struct<dataset_name string, table_name string>;
  declare get_labels default '';

  call sdp_internal.parse_table_name(p_table_name, table_info);

  execute immediate """
    select ifnull(any_value(option_value), '')
    from `"""||table_info.dataset_name||""".INFORMATION_SCHEMA.TABLE_OPTIONS`
    where table_name = '"""||table_info.table_name||"""'
      and option_name = 'labels'"""
    into get_labels;

  if length(get_labels) > 0 then
    execute immediate "select " || get_labels into p_labels_array;
  else
    set p_labels_array = [];
  end if;
end
