create or replace function table_exists(table_name text) returns boolean as $$
  declare
  begin
    return EXISTS (
      SELECT 1
      FROM   pg_catalog.pg_class c
      JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      WHERE  n.nspname = ANY(current_schemas(FALSE))
      AND    lower(c.relname) similar to lower(table_name)
      AND    c.relkind = 'r');
  end
$$ language plpgsql;

create or replace function function_exists(func_name text, paramtypes text) returns boolean as $$
  declare
  begin
    return EXISTS (
      SELECT 1
      FROM   pg_proc
      WHERE  proname = func_name
      AND    proargtypes :: text similar to paramtypes);
  end
$$ language plpgsql;

create or replace function column_exists(table_npat text, column_npat text) returns boolean as $$
  declare
  begin
   return EXISTS (
      SELECT column_name 
      FROM information_schema.columns
      WHERE lower(table_name) similar to lower(table_npat) and lower(column_name) similar to lower(column_npat));
  end
$$ language plpgsql;

create or replace function check_function_not_exists(func_name text, paramtypes text) returns void as $$
  declare
    col_name text;
  begin
    if function_exists(func_name, paramtypes) then
      RAISE 'This application may overwrite function % which already exists', func_name;
    end if;
  end
$$ language plpgsql;

create or replace function check_table_exists(table_name text, col_names text[]) returns void as $$
  declare
    col_name text;
  begin
    if not table_exists(table_name) then
      RAISE 'This application depends on table % created by another application', table_name;
    end if;
    foreach col_name in array col_names loop
      if not column_exists(table_name, col_name) then
        RAISE 'This application depends on column %(%) created by another application', table_name, col_name;
      end if;
    end loop;
  end
$$ language plpgsql;

create or replace function check_table_not_exists(table_name text) returns void as $$
  declare
    tn text;
  begin
    if table_exists(table_name) then
      for tn in SELECT c.relname
        FROM   pg_catalog.pg_class c
        JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE  n.nspname = ANY(current_schemas(FALSE))
        AND    lower(c.relname) similar to lower(table_name)
        AND    c.relkind = 'r' loop
        RAISE 'This application may overwrite table % which already exists', tn;
      end loop;
    end if;
  end
$$ language plpgsql;

do $$ begin
  perform check_table_exists('cmaq\_7da\_DES', array['col','row','date','DESpm\_7da','DESo\_7da']);
  perform check_table_exists('observation\_fact', array['patient\_num','encounter\_num','concept\_cd','nval_num','start\_date']);
  perform check_table_exists('patient\_dimension', array['patient\_num', 'birth\_date', 'sex\_cd', 'race\_cd']);
  perform check_table_exists('visit\_dimension', array['patient\_num', 'encounter\_num', 'inout\_cd']);
  perform check_table_not_exists('%reduced%');
  perform check_table_not_exists('%asthma%');
  perform check_table_not_exists('features');
  perform check_function_not_exists('filter_icd', '25');
  perform check_function_not_exists('latlon2rowcol', '1700 1700 23');
end $$;


