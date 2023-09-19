create or replace procedure do_full_refresh(
  _in_table_name string
)
options (strict_mode=false)
begin
  call set_table_label(
    _in_table_name,
    'full-refresh',
    'true'
  );
end
