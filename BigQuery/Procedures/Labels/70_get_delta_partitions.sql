create or replace procedure get_delta_partitions (
  _in_derived_table struct<
    table_name string,
    ingest_timestamp_column_name string
  >,
  _in_base_table struct<
    table_name string,
    partition_column_name string,
    ingest_timestamp_column_name string
  >,
  out _out_delta_info_struct struct<
    max_ingest_ts timestamp,
    dates_array array<date>
  >
)

begin
  call get_delta_partitions_ext(
    _in_derived_table,
    (
      _in_base_table.table_name,
      _in_base_table.partition_column_name,
      _in_base_table.ingest_timestamp_column_name,
      null
    ),
    _out_delta_info_struct
  );
end
