create procedure get_delta_date_partitions_filter_src(
  _in_tgt struct<
    table_name string,
    event_timestamp_column_name string
  >,
  _in_src struct<
    table_name string,
    event_timestamp_column_name string,
    additional_filter_condition string
  >,
  
  out _out_partitions_struct struct<
    max_tgt_ts timestamp,
    dates array<date>
  >
)
options(strict_mode=false)
begin
  call get_delta_date_partitions_filter_src_custom_labels(
    (_in_tgt.table_name, _in_tgt.event_timestamp_column_name, 'last-job-start', 'max-tgt-ts'),
    _in_src,
    _out_partitions_struct
  );
end
