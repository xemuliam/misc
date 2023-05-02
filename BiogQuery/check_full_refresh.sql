create procedure check_full_refresh(_in_table_name string, out _out_do_full_refresh bool)
options(
  strict_mode=false)
begin
  declare full_refresh_str string default false;

  call get_table_label(
    _in_table_name, 'full-refresh', full_refresh_str
  );
  
  if full_refresh_str = 'true' then
    set _out_do_full_refresh = true;
  else
    set _out_do_full_refresh = false;
  end if;
end
