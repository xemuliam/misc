create or replace procedure remove_table_label (
  _in_table_name string,
  _in_label_name string
)

begin
  call set_table_label(_in_table_name, _in_label_name, cast(null as string));
end
