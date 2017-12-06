-- doens't work if have more than 1664 columns (postgres 10 limit)
drop extension if exists tablefunc;
create extension tablefunc;

create or replace function dropfunction(funcname text) returns void as $$
declare
  sql text;
begin
  for sql in SELECT 'DROP FUNCTION ' || oid::regprocedure
             FROM   pg_proc
	     WHERE  proname = funcname
	     AND    pg_function_is_visible(oid)
	     loop
    raise notice 'sql: %', sql;
    execute sql;
  end loop;
end; $$ language plpgsql;

create or replace function executesql(sql text)
  returns void
  set client_min_messages = error
  language plpgsql
  as $$
begin
  execute sql;
end; $$;

create or replace function longtowide(table_name_long text, keys text[], keytypes text[], colnamecol text, col_value_cols text[], col_value_col_types text[], col_value_col_suffixes text[], table_name_wide text) returns void as $$
declare
  colnames text[];
  table_name_wide_value text;
  table_name_wide_values text[];
  col_name_wide_value text;
  col_name_wide_values text[];
  table_name_wide_valuei text;
  table_name_wide_valueis text[];
  col_name_wide_valuei text;
  col_name_wide_valueis text[];
  table_name_wide_meta text;
  sql text;
  keyargs text;
  valueargs text;
  valueargscolname text;
  ncols integer;
  starti integer;
  endi integer;
  suffix text;
  colnamescurr text[];
  san text;
begin
  colnames := getcolnames(table_name_long, colnamecol);
  keyargs := array_to_string(array(select format('%I', key) from unnest(keys) as keys(key)),',');
  table_name_wide_meta := table_name_wide || '_meta';

  perform executesql(format('drop table if exists %I', table_name_wide_meta));
  sql := format('create table %I as select $1 colnames', table_name_wide_meta);
  execute sql using colnames;

  ncols := array_upper(colnames,1);
  -- 1000 columns at a time to prevent column number > 1600
  table_name_wide_values := ARRAY[] :: text[];
  col_name_wide_values := ARRAY[] :: text[];
  for i in 1 .. array_upper(col_value_cols,1) loop
    table_name_wide_value := table_name_wide || col_value_col_suffixes[i];
    table_name_wide_values := table_name_wide_values || table_name_wide_value;
    col_name_wide_value := col_value_col_suffixes[i];
    col_name_wide_values := col_name_wide_values || col_name_wide_value;
    table_name_wide_valueis := ARRAY[] :: text[];
    col_name_wide_valueis := ARRAY[] :: text[];
    for starti in 1 .. ncols + 1 by 1000 loop 
      endi := least(starti + 1000 - 1, ncols);
      suffix := starti :: text;
      raise notice 'processing: %, % - %', col_value_cols[i], starti, endi;
      table_name_wide_valuei := table_name_wide_value || suffix;
      table_name_wide_valueis := table_name_wide_valueis || table_name_wide_valuei;
      col_name_wide_valuei := col_value_col_suffixes[i] || suffix;
      col_name_wide_valueis := col_name_wide_valueis || col_name_wide_valuei;
      colnamescurr := colnames[starti:endi];
      perform executesql(format('drop table if exists %I', table_name_wide_value));
      valueargscolname := col_value_col_suffixes[i] || suffix; -- array_to_string(array(select format('%s', colname || col_value_col_suffixes[i]) from unnest(colnames) as colnames(colname)),',');
      valueargs := array_to_string(array(select format('%I', colname || suffix) from unnest(colnamescurr) as colnames(colname)),',');
      perform executesql(format('drop table if exists %I', table_name_wide_valuei)); 
      sql := format('create table %I as select %s, ARRAY[%s] %I from (select * from crosstab(''select ARRAY[%s], %I, %I from %I'',''select * from unnest(ARRAY[%s])'') as tmp(tmpkey integer[], %s)) a',
                   table_name_wide_valuei,
		   array_to_string(array(select format('tmpkey[%s] %I', n, keys[n]) from generate_series(1, array_upper(keys,1)) as ns(n)),','),
		   valueargs,
           	   col_name_wide_valuei,
		   keyargs,
		   colnamecol,
		   col_value_cols[i],
                   table_name_long,
		   array_to_string(array(select format('''%L''', colname) from unnest(colnamescurr) as colnames(colname)),','),
		   array_to_string(array(select format('%I %s', colname || suffix, col_value_col_types[i]) from unnest(colnamescurr) as colnames(colname)),',')
		   );
--      raise notice 'sql: %', sql;
      execute sql;
    end loop;
    execute format('select sparse_array_name(ARRAY[] :: %s[])', col_value_col_types[i]) into san;
    perform executesql(format('drop table if exists %I', table_name_wide_value));
    sql := format('create table %I as select %s, (%s :: %s) %I from %I%s',
                table_name_wide_value,
		keyargs,
		array_to_string(array(select format('%I', cni) from unnest(col_name_wide_valueis) as col_name_wide_valueis(cni)),' || '),
		san,
		col_name_wide_value,
		table_name_wide_valueis[1],
		array_to_string(array(select format(' inner join %I using (%s)', tni, keyargs) from unnest(table_name_wide_valueis[2:array_upper(table_name_wide_valueis,1)]) as table_name_wide_valueis(tni)),''));
    execute sql;

    for i in 1 .. array_upper(table_name_wide_valueis,1) loop
      perform executesql(format('drop table %I', table_name_wide_valueis[i]));
    end loop;
  end loop;

  perform executesql(format('drop table if exists %I', table_name_wide));
  sql := format('create table %I as select * from %I%s',
                table_name_wide,
		table_name_wide_values[1],
		array_to_string(array(select format(' inner join %I using (%s)', tn, keyargs) from unnest(table_name_wide_values[2:array_upper(table_name_wide_values,1)]) as table_name_wide_values(tn)),''));
  execute sql;

  for i in 1 .. array_upper(table_name_wide_values,1) loop
    perform executesql(format('drop table %I', table_name_wide_values[i]));
  end loop;

end; $$ language plpgsql;

create or replace function getcolnames(table_name text, colnamecol text) returns text[] as $$
  begin
    return ARRAY(select * from getcolnamestable(table_name, colnamecol));
  end;
$$ language plpgsql;

create or replace function getcolnamestable(table_name text, colnamecol text) returns table(colname text) as $$
  begin
    return query execute format('select distinct (%I :: text) from %I order by %I', colnamecol, table_name, colnamecol);
  end;
$$ language plpgsql;
  
  

