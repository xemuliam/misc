create procedure get_delta_partitions_filter_tgt(_in_tgt struct<table_name string, event_timestamp_column_name string, additional_filter_condition string>, _in_src struct<table_name string, event_timestamp_column_name string>, out _out_partitions_struct struct<max_tgt_ts timestamp, dates array<date>>)
options(
  strict_mode=false)
begin
  call get_delta_partitions_filter_tgt_custom_labels(
    (_in_tgt.table_name, _in_tgt.event_timestamp_column_name, _in_tgt.additional_filter_condition, null, null),
    _in_src,
    _out_partitions_struct
  );
end
