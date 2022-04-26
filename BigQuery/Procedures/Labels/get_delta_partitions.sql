create or replace procedure get_delta_partitions(
  p_ard STRUCT<table_name STRING,
  event_timestamp_column_name STRING>,
  p_pdm STRUCT<table_name STRING,
  partition_column_name STRING,
  event_timestamp_column_name STRING>,
  
  OUT p_partitions_struct STRUCT<max_ard_ts TIMESTAMP, dates ARRAY<DATE>>
)
begin
  DECLARE max_ard_ts_str, last_job_start_str string DEFAULT null;
  DECLARE max_ard_ts timestamp DEFAULT null;
  declare pdm_partitions struct<count_all int64, dates array<date>>;
  declare labels array<struct<name string, value string>> default [];
  declare pdm struct<dataset_name string, table_name string>;

  -- parse pdm table name into structure
  call sdp_internal.parse_table_name(
    p_pdm.table_name,
    pdm
  );
  -- attempt to get ard table labels
  call sdp_internal.get_many_table_labels(
    p_ard.table_name,
    ['last-job-start', 'max-ard-ts'],
    labels
  );
  -- get max-ard-ts value from label
  set max_ard_ts_str = (
    select any_value(value)
    from unnest(labels)
    where name = 'max-ard-ts'
  );
  -- if max ard ts is empty then get value from real data
  if max_ard_ts_str is not null then
    SET max_ard_ts = parse_timestamp('%Y-%m-%d__%H-%M-%S', max_ard_ts_str);
  else
    execute immediate """
      SELECT ifnull(max("""||p_ard.event_timestamp_column_name||"""), TIMESTAMP('1991-08-24'))
      FROM `"""||p_ard.table_name||"""`
    """ into max_ard_ts;
  end if;
  -- get last-job-start value from label
  set last_job_start_str = (
    select any_value(value)
    from unnest(labels)
    where name = 'last-job-start'
  );

  -- get partitions from metadata using last job start label
  if last_job_start_str is not null then
    execute immediate """
      select (count(1), array_agg(parse_date('%Y%m%d', if(regexp_contains(partition_id, r'^\\d{8}$'), partition_id, null)) ignore nulls))
      from `"""||pdm.dataset_name||""".INFORMATION_SCHEMA.PARTITIONS`
      where table_name = '"""||pdm.table_name||"""'
      and storage_tier = 'ACTIVE'
      and last_modified_time > parse_timestamp('%Y-%m-%d__%H-%M-%S', '"""||last_job_start_str||"""')
    """ into pdm_partitions;
  end if;

  -- if no label or no "numeric" pertitions then get partitoons from the real data
  if last_job_start_str is null or pdm_partitions.count_all > ifnull(array_length(pdm_partitions.dates), 0) then
    execute immediate """
      select (-1,
        (
          SELECT ARRAY_AGG(DISTINCT DATE("""||p_pdm.partition_column_name||"""))
          FROM `"""||p_pdm.table_name||"""`
          WHERE """||p_pdm.event_timestamp_column_name||""" > TIMESTAMP_SUB('"""||max_ard_ts||"""', INTERVAL 100 MINUTE)
        )
      )
    """ into pdm_partitions;
  end if;

  set p_partitions_struct = (max_ard_ts, pdm_partitions.dates);
end
