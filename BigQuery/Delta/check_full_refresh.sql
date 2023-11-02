create procedure check_full_refresh(
  _in_table_name string,
  
  out _out_do_full_refresh bool
)
options(strict_mode=false)
begin
  call should_full_refresh(_in_table_name, _out_do_full_refresh);
end
