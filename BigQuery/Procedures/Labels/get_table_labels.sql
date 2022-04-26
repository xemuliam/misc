create or replace procedure sdp_internal.get_table_label(p_table_name STRING, p_label_name STRING, OUT p_label_value STRING)
begin
  declare labels array<struct<name string, value string>> default [];

  call sdp_internal.get_many_table_labels(p_table_name, [p_label_name], labels);
  set p_label_value = labels[safe_offset(0)].value;
end
