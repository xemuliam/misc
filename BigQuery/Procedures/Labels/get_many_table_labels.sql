create or replace procedure get_many_table_labels(p_table_name STRING, p_label_names_array ARRAY<STRING>, OUT p_labels_array ARRAY<STRUCT<name STRING, value STRING>>)
begin
  declare labels array<struct<name string, value string>> default [];

  call sdp_internal.get_all_table_labels(p_table_name, labels);

  set p_labels_array = (
    select array(
      select as struct l.*
      from unnest(labels) l
      where name in (
        select n
        from unnest(p_label_names_array) n
      )
    )
  );
end
