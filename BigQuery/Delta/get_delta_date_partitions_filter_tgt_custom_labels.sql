create procedure get_delta_date_partitions_filter_tgt_custom_labels(
  _in_tgt struct<
    table_name string,
    event_timestamp_column_name string,
    additional_filter_condition string,
    last_job_start_label_name string,
    max_tgt_ts_label_name string
  >,
  _in_src struct<
    table_name string,
    event_timestamp_column_name string
  >,
  
  out _out_partitions_struct struct<
    max_tgt_ts timestamp,
    dates array<date>
  >
)
options(strict_mode=false)
begin
  call get_delta_date_partitions_ext(
    _in_tgt,
    (_in_src.table_name, _in_src.event_timestamp_column_name, null),
    _out_partitions_struct
  );
end
