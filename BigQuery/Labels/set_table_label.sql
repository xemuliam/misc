create procedure set_table_label(
  _in_table_name string,
  _in_label_name string,
  _in_label_value string
)
options(strict_mode=false)
begin
  call set_many_table_labels(_in_table_name, [(_in_label_name, _in_label_value)]);
end
