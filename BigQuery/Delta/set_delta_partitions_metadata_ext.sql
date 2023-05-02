create procedure set_delta_partitions_metadata_ext(_in_table_name string, _in_delta_metadata_struct struct<last_job_start array<struct<name string, value timestamp>>, max_tgt_ts array<struct<name string, value timestamp>>>)
options(
  strict_mode=false)
begin
  call set_many_table_labels(
    _in_table_name,
    (select array(
      select as struct name, format_timestamp('%y-%m-%d__%h-%m-%s', value) value
      from unnest(_in_delta_metadata_struct.last_job_start)
    )) ||
    (select array(
      select as struct name, format_timestamp('%y-%m-%d__%h-%m-%s', value) value
      from unnest(_in_delta_metadata_struct.max_tgt_ts)
    ))
  );
end
