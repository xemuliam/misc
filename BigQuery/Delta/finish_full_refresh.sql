create procedure finish_full_refresh(
  _in_table_name string
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

  if full_refresh_started_str = 'true' then
    call set_many_table_labels(
      _in_table_name,
      [
        ('full-refresh', format_timestamp('%Y-%m-%d__%H-%M-%S', current_timestamp)),
        ('full-refresh-started', cast(null as string))
      ]
    );
    set full_refresh_str = 'clean';
  end if;

  if full_refresh_str = 'true' then
    call set_table_label(_in_table_name, 'full-refresh-started', 'true');
  end if;
end