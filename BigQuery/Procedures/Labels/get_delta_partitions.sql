create or replace procedure get_delta_partitions(
  _in_derived_table STRUCT<
    table_name STRING,
    event_timestamp_column_name STRING
  >,
  _in_in_base_table STRUCT<
    table_name STRING,
    partition_column_name STRING,
    event_timestamp_column_name STRING
  >,
  OUT _out_delta_info_struct STRUCT<
    max_event_ts TIMESTAMP,
    dates_array ARRAY<DATE>
  >
)
begin
  DECLARE max_event_ts_str, last_batch_start_str string DEFAULT null;
  DECLARE max_event_ts timestamp DEFAULT null;
  declare base_table_partitions struct<count_all int64, dates_array array<date>>;
  declare labels array<struct<name string, value string>> default [];
  declare base_table_name_struct struct<dataset_name string, table_name string>;

  -- parse base table name into structure
  call parse_table_name(
    _in_base_table.table_name,
    base_table_name_struct
  );
  -- attempt to get ard table labels
  call get_many_table_labels(
    _in_derived_table.table_name,
    ['last-job-start-ts', 'max-event-ts'],
    labels
  );
  -- get max-event-ts value from label
  set max_event_ts_str = (
    select any_value(value)
    from unnest(labels)
    where name = 'max-event-ts'
  );
  -- if max ard ts is empty then get value from real data
  if max_event_ts_str is not null then
    SET max_event_ts = parse_timestamp('%Y-%m-%d__%H-%M-%S', max_event_ts_str);
  else
    execute immediate """
      SELECT ifnull(max("""||_in_derived_table.event_timestamp_column_name||"""), TIMESTAMP('1991-08-24'))
      FROM `"""||_in_derived_table.table_name||"""`
    """ into max_event_ts;
  end if;
  -- get last-job-start-ts value from label
  set last_batch_start_str = (
    select any_value(value)
    from unnest(labels)
    where name = 'last-job-start-ts'
  );

  -- get partitions from metadata using last job start label
  if last_batch_start_str is not null then
    execute immediate """
      select (count(1), array_agg(parse_date('%Y%m%d', if(regexp_contains(partition_id, r'^\\d{8}$'), partition_id, null)) ignore nulls))
      from `"""||base_table_name_struct.dataset_name||""".INFORMATION_SCHEMA.PARTITIONS`
      where table_name = '"""||base_table_name_struct.table_name||"""'
      and storage_tier = 'ACTIVE'
      and last_modified_time > parse_timestamp('%Y-%m-%d__%H-%M-%S', '"""||last_batch_start_str||"""')
    """ into base_table_partitions;
  end if;

  -- if no label or no "numeric" pertitions then get partitoons from the real data
  if last_batch_start_str is null or base_table_partitions.count_all > ifnull(array_length(base_table_partitions.dates_array), 0) then
    execute immediate """
      select (-1,
        (
          SELECT ARRAY_AGG(DISTINCT DATE("""||_in_base_table.partition_column_name||"""))
          FROM `"""||_in_base_table.table_name||"""`
          WHERE """||_in_base_table.event_timestamp_column_name||""" > TIMESTAMP_SUB('"""||max_event_ts||"""', INTERVAL 100 MINUTE)
        )
      )
    """ into base_table_partitions;
  end if;

  set _out_delta_info_struct = (max_event_ts, base_table_partitions.dates_array);
end
