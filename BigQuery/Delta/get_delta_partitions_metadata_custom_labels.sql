create procedure get_delta_partitions_metadata_custom_labels(_in_table_name string, _in_last_job_start_label_name string, _in_max_tgt_ts_label_name string, out _out_delta_metadata_struct struct<last_job_start struct<name string, value timestamp>, max_tgt_ts struct<name string, value timestamp>>)
options(
  strict_mode=false)
begin
  declare delta_metadata_struct struct<last_job_start array<struct<name string, value timestamp>>, max_tgt_ts array<struct<name string, value timestamp>>>;

  call get_delta_partitions_metadata_ext(
    _in_table_name,
    [_in_last_job_start_label_name],
    [_in_max_tgt_ts_label_name],
    delta_metadata_struct
  );

  set _out_delta_metadata_struct = (
    if(
      array_length(delta_metadata_struct.last_job_start) = 1 and delta_metadata_struct.last_job_start[offset(0)].name = _in_last_job_start_label_name,
      (delta_metadata_struct.last_job_start[offset(0)].name, delta_metadata_struct.last_job_start[offset(0)].value), null
    ),
    if(
      array_length(delta_metadata_struct.max_tgt_ts) = 1 and delta_metadata_struct.max_tgt_ts[offset(0)].name = _in_max_tgt_ts_label_name,
      (delta_metadata_struct.max_tgt_ts[offset(0)].name, delta_metadata_struct.max_tgt_ts[offset(0)].value), null
    )
  );
end
