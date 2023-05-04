create procedure finish_full_refresh(
  _in_table_name string
)
options(strict_mode=false)
begin
  call set_table_label(
    _in_table_name,
    'full-refresh',
    format_timestamp('%y-%m-%d__%h-%m-%s', current_timestamp)
  );
end
