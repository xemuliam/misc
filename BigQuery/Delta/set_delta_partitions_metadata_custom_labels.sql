create procedure set_delta_partitions_metadata_custom_labels(_in_table_name string, _in_current_job_start struct<name string, value timestamp>, _in_max_tgt_ts struct<name string, value timestamp>)
options(
  strict_mode=false)
begin
  call set_delta_partitions_metadata_ext(
    _in_table_name,
    (
      [(_in_current_job_start)],
      [(_in_max_tgt_ts)]
    )
  );
end
