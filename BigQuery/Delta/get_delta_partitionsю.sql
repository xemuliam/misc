create procedure get_delta_partitions(_in_tgt struct<table_name string, event_timestamp_column_name string>, _in_src struct<table_name string, event_timestamp_column_name string>, out _out_partitions_struct struct<max_tgt_ts timestamp, dates array<date>>)
options(
  strict_mode=false)
begin
  call get_delta_partitions_ext(
    (_in_tgt.table_name, _in_tgt.event_timestamp_column_name, null, null, null),
    (_in_src.table_name, _in_src.event_timestamp_column_name, null),
    _out_partitions_struct
  );
end

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
