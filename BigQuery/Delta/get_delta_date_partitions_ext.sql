create procedure get_delta_date_partitions_ext(
  _in_tgt struct<
    table_name string,
    event_timestamp_column_name string,
    additional_filter_condition string,
    last_job_start_name string,
    max_tgt_ts_name string
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
  declare max_tgt_ts, last_job_start timestamp default null;
  declare src_partition_column_name string;
  declare src_partitions struct<count_all int64, dates array<date>>;
  declare src struct<dataset_name string, table_name string>;
  declare delta_metadata struct<last_job_start struct<name string, value timestamp>, max_tgt_ts struct<name string, value timestamp>>;

  -- parse src table name into structure
  call parse_table_name(
    _in_src.table_name,
    src
  );

  -- get delta partitions metadata
  call get_delta_date_partitions_metadata_custom_labels(
    _in_tgt.table_name,
    ifnull(nullif(_in_tgt.last_job_start_name, ''), 'last-job-start'),
    ifnull(nullif(_in_tgt.max_tgt_ts_name, ''), 'max-tgt-ts'),
    delta_metadata
  );

  -- identify partitioning column
  execute immediate """
    select ifnull(column_name, 'no_partitioning_field')
    from `"""||src.dataset_name||""".INFORMATION_SCHEMA.COLUMNS`
    where table_name = '"""||src.table_name||"""'
      and is_partitioning_column = 'YES'
  """ into src_partition_column_name;

  -- get max tgt timestamp from real data either if we have filter for tgt
  -- or if corresponding label is empty
  if ifnull(_in_tgt.additional_filter_condition, '') <> '' or delta_metadata.max_tgt_ts.value is null then
    execute immediate """
      select ifnull(max("""||_in_tgt.event_timestamp_column_name||"""), TIMESTAMP('1991-08-24'))
      from `"""||_in_tgt.table_name||"""`
    """||ifnull("  where "||_in_tgt.additional_filter_condition, '') into max_tgt_ts;
  else
    SET max_tgt_ts = delta_metadata.max_tgt_ts.value;
  end if;

  -- get partitions from metadata using last job either last job start label (if available)
  -- or max tgt timestamp calculated on previous step minus streaming buffer length
  execute immediate """
    select (count(1), array_agg(
      coalesce(safe.parse_date('%Y', partition_id), safe.parse_date('%Y%m', partition_id), safe.parse_date('%Y%m%d', partition_id))
      ignore nulls))
    from `"""||src.dataset_name||""".INFORMATION_SCHEMA.PARTITIONS`
    where table_name = '"""||src.table_name||"""'
    and last_modified_time > '"""||ifnull(delta_metadata.last_job_start.value, timestamp_sub(max_tgt_ts, interval 100 minute))||"""'
  """ into src_partitions;

  -- get partitions from real data either if we have filter for src
  -- or if we can't identifiy exact partitions list from metadata
  if ifnull(_in_src.additional_filter_condition, '') <> '' or src_partitions.count_all > ifnull(array_length(src_partitions.dates), 0) then
    execute immediate """
      select (-1,
        (
          select array_agg(distinct date("""||src_partition_column_name||""") ignore nulls)
          from `"""||src.dataset_name||'.'||src.table_name||"""`
          where true"""||
            if(src_partitions.count_all > ifnull(array_length(src_partitions.dates), 0), '',
              " and date("||src_partition_column_name||") in ("||
              (select ifnull(string_agg("'"||d||"'", ', '), "'1991-08-24'") from unnest(src_partitions.dates) d)||")")||
            ifnull(" and "||nullif(ltrim(rtrim(_in_src.additional_filter_condition)), ''), '')||"""
            and """||_in_src.event_timestamp_column_name||""" > timestamp_sub('"""||max_tgt_ts||"""', interval 100 minute)
        )
      )
    """ into src_partitions;
  end if;

  set _out_partitions_struct = (max_tgt_ts, src_partitions.dates);
end
