create or replace procedure remove_table_label(p_table_name STRING, p_label_name STRING)
begin
  call sdp_internal.set_table_label(p_table_name, p_label_name, cast(null as string));
end
