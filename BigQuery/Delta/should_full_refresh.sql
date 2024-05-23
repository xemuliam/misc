create procedure should_full_refresh(
  _in_table_name string,
  
  out _out_do_full_refresh bool
)
options(strict_mode=false)
begin
  declare full_refresh_labels_arr ARRAY<STRUCT<name STRING, value STRING>> default null;
  declare full_refresh_str, full_refresh_started_str string default null;

  call get_many_table_labels(
    _in_table_name, ['full-refresh', 'full-refresh-started'], full_refresh_labels_arr
  );

  set full_refresh_str = (
      select value from unnest(full_refresh_labels_arr) where name = 'full-refresh'
  );
  set full_refresh_started_str = (
      select value from unnest(full_refresh_labels_arr) where name = 'full-refresh-started'
  );

  if full_refresh_str = 'true' and full_refresh_started_str = 'true' then
    set _out_do_full_refresh = true;
  else
    set _out_do_full_refresh = false;
  end if;
end