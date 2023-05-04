create procedure set_delta_date_partitions_metadata(
  _in_table_name string,
  _in_current_job_start timestamp,
  _in_max_tgt_ts timestamp
)
options(strict_mode=false)
begin
  call set_delta_date_partitions_metadata_custom_labels(
    _in_table_name,
    ('last-job-start', _in_current_job_start),
    ('max-tgt-ts', _in_max_tgt_ts)
  );
end
