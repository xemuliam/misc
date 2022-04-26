create or replace procedure set_table_label(
  _in_table_name STRING,
  _in_label_name STRING,
  _in_label_value STRING
)
begin
  call set_many_table_labels(_in_table_name, [(_in_label_name, _in_label_value)]);
end
