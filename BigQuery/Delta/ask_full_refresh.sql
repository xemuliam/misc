CREATE OR REPLACE PROCEDURE do_full_refresh(
  _in_table_name STRING
)
OPTIONS (strict_mode=false)
begin
  call set_table_label(
    _in_table_name,
    'full-refresh',
    'true'
  );
end
