create or replace procedure begin_full_refresh(
  _in_table_name string
)
options (strict_mode=false)
begin
  call set_table_label(
    _in_table_name,
    'full-refresh-started',
    'true'
  );
end