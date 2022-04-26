create or replace procedure set_table_label(p_table_name STRING, p_label_name STRING, p_label_value STRING)
begin
  call sdp_internal.set_many_table_labels(p_table_name, [(p_label_name, p_label_value)]);
end
