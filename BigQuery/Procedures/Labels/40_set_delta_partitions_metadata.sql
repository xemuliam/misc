create or replace procedure set_many_table_labels (
  _in_table_name string,
  _in_last_batch_start_ts timestamp,
  _in_max_ingest_ts timestamp
)

begin
  call set_many_table_labels` (
    _in_table_name,
    [
      ('last-batch-start-ts', format_timestamp('%Y-%m-%d__%H-%M-%S', _in_last_batch_start_ts)),
      ('max-ingest-ts', format_timestamp('%Y-%m-%d__%H-%M-%S', _in_max_ingest_ts))
    ]
  );
end
