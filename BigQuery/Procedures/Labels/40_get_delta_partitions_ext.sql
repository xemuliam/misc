create or replace procedure get_delta_partitions_ext (
  _in_derived_table struct<
    table_name string,
    ingest_timestamp_column_name string
  >,
  _in_base_table struct<
    table_name string,
    partition_column_name string,
    ingest_timestamp_column_name string,
    additional_filter_condition string
  >,
  out _out_delta_info_struct struct<
    max_ingest_ts timestamp,
    dates_array array<date>
  >
)

begin
  declare max_ingest_ts, last_batch_start_ts timestamp default null;
  declare base_table_partitions struct<count_all int64, dates array<date>>;
  declare base_table_name_struct struct<dataset_name string, table_name string>;
  declare delta_metadata_struct struct<last_batch_start_ts timestamp, max_ingest_ts timestamp>;

  -- parse base table name into structure
  call parse_table_name(
    _in_base_table.table_name,
    base_table_name_struct
  );
  -- get delta partitions metadata
  call get_delta_partitions_metadata(
    _in_derived_table.table_name,
    delta_metadata_struct
  );

  -- if max derived ts is empty then get value from real data
  if delta_metadata_struct.max_ingest_ts is not null then
    set max_ingest_ts = delta_metadata_struct.max_ingest_ts;
  else
    execute immediate """
      select ifnull(max("""||_in_derived_table.ingest_timestamp_column_name||"""), timestamp('1991-08-24'))
      from `"""||_in_derived_table.table_name||"""`
    """ into max_ingest_ts;
  end if;

  -- get partitions from metadata using last batch start label
  if delta_metadata_struct.last_batch_start_ts is not null then
    execute immediate """
      select (count(1), array_agg(parse_date('%y%m%d', if(regexp_contains(partition_id, r'^\\d{8}$'), partition_id, null)) ignore nulls))
      from `"""||base_table_name_struct.dataset_name||""".information_schema.partitions`
      where table_name = '"""||base_table_name_struct.table_name||"""'
      and storage_tier = 'active'
      and last_modified_time > '"""||delta_metadata_struct.last_batch_start_ts||"""'
    """ into base_table_partitions;
  end if;

  -- if no label or no "numeric" partitions then get partitions from the real data
  if delta_metadata_struct.last_batch_start_ts is null or base_table_partitions.count_all > ifnull(array_length(base_table_partitions.dates_array), 0) then
    execute immediate """
      select (-1,
        (
          select array_agg(distinct date("""||_in_base_table.partition_column_name||"""))
          from `"""||_in_base_table.table_name||"""`
          where true"""||ifnull( " and "||_in_base_table.additional_filter_condition, '')||"""
            and """||_in_base_table.ingest_timestamp_column_name||""" > timestamp_sub('"""||max_ingest_ts||"""', interval 100 minute)
        )
      )
    """ into base_table_partitions;
  end if;

  set _out_delta_info_struct = (max_ingest_ts, base_table_partitions.dates_array);
end
