create or replace procedure get_table_label(
  _in_table_name STRING,
  _in_label_name STRING,
  OUT _out_label_value STRING)
begin
  declare labels array<struct<name string, value string>> default [];

  call get_many_table_labels(_in_table_name, [_in_label_name], labels);
  set _out_label_value = labels[safe_offset(0)].value;
end
