create procedure finish_full_refresh(
  _in_table_name string
)
options(strict_mode=false)
begin
  declare full_refresh_started_str string default null;

  call get_table_label(
    _in_table_name, 'full-refresh-started', full_refresh_started_str
  );
  
  if full_refresh_started_str = 'true' then
    call set_many_table_labels(
      _in_table_name,
      [
        ('full-refresh', format_timestamp('%Y-%m-%d__%H-%M-%S', current_timestamp)),
        ('full-refresh-started', cast(null as string))
      ]
    );
  end if;
end
