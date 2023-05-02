create procedure remove_table_label(_in_table_name string, _in_label_name string)
options(
  strict_mode=false)
begin
  call set_table_label(_in_table_name, _in_label_name, cast(null as string));
end
