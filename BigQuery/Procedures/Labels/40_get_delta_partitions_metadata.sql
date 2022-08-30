create or replace procedure set_delta_partitions_metadata (
  _in_table_name string,

  out _out_delta_metadata_struct STRUCT<
    last_job_start TIMESTAMP,
    max_ard_ts TIMESTAMP
  >
)
begin
  declare labels array<struct<name string, value string>> default [];

  call sdp_internal.get_many_table_labels(
    _in_table_name,
    ['last-job-start', 'max-ard-ts'],
    labels
  );

  set _out_delta_metadata_struct = (
    safe.parse_timestamp('%Y-%m-%d__%H-%M-%S', (select any_value(value) from unnest(labels) where name = 'last-job-start')),
    safe.parse_timestamp('%Y-%m-%d__%H-%M-%S', (select any_value(value) from unnest(labels) where name = 'max-ard-ts'))
  );
end
