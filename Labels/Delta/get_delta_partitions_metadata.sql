create procedure get_delta_partitions_metadata(_in_table_name string, out _out_delta_metadata_struct struct<last_job_start timestamp, max_tgt_ts timestamp>)
options(
  strict_mode=false)
begin
  declare delta_metadata_struct struct<last_job_start struct<name string, value timestamp>, max_tgt_ts struct<name string, value timestamp>>;

  call get_delta_partitions_metadata_custom_labels(
    _in_table_name,
    'last-job-start',
    'max-tgt-ts',
    delta_metadata_struct
  );

  set _out_delta_metadata_struct = (
    delta_metadata_struct.last_job_start.value,
    delta_metadata_struct.max_tgt_ts.value
  );
end
