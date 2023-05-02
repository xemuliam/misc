create procedure get_delta_partitions_metadata_ext(_in_table_name string, _in_last_job_start_label_name_array array<string>, _in_max_tgt_ts_label_name_array array<string>, out _out_delta_metadata_struct struct<last_job_start array<struct<name string, value timestamp>>, max_tgt_ts array<struct<name string, value timestamp>>>)
options(
  strict_mode=false)
begin
  declare labels array<struct<name string, value string>> default [];

  call get_many_table_labels(
    _in_table_name,
    ifnull(_in_last_job_start_label_name_array, []) || ifnull(_in_max_tgt_ts_label_name_array, []),
    labels
  );

  set _out_delta_metadata_struct = (
    (select array(
      select as struct name, safe.parse_timestamp('%y-%m-%d__%h-%m-%s', value) value
      from unnest(labels) l where name in (select n from unnest(_in_last_job_start_label_name_array) n)
    )),
    (select array(
      select as struct name, safe.parse_timestamp('%y-%m-%d__%h-%m-%s', value) value
      from unnest(labels) l where name in (select n from unnest(_in_max_tgt_ts_label_name_array) n)
    ))
  );
end
