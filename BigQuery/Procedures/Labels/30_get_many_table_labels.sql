create or replace procedure get_many_table_labels (
  _in_table_name string,
  _in_label_names_array array<string>,
  
  out _out_labels_array array<
    struct<
      name string,
      value string
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
