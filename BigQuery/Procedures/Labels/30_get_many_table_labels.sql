create or replace procedure get_many_table_labels (
  _in_table_name STRING,
  _in_label_names_array ARRAY<STRING>,
  OUT _out_labels_array ARRAY<
    STRUCT<
      name STRING,
      value STRING
    >
  >
)

begin
  declare labels array<struct<name string, value string>> default [];

  call get_all_table_labels(_in_table_name, labels);

  set _out_labels_array = (
    select array(
      select as struct l.*
      from unnest(labels) l
      where name in (
        select n from unnest(_in_label_names_array) n
      )
    )
  );
end
