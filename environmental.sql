create or replace function DESpm(pm25 numeric) returns numeric as $$
declare
begin
  if pm25 < 4.0 then
      return 1;
  elsif pm25 < 7.07 then
      return 2;
  elsif pm25 < 8.98 then
      return 3;
  elsif pm25 < 11.37 then
      return 4;
  else
      return 5;
  end if;
end;
$$ language plpgsql;

create or replace function DESo(o3_ppb numeric) returns numeric as $$
declare
begin
  if o3_ppb < 50 then
      return 1;
  elsif o3_ppb < 76 then
      return 2;
  elsif o3_ppb < 101 then
      return 3;
  elsif o3_ppb < 126 then
      return 4;
  else
      return 5;
  end if;
end;
$$ language plpgsql;

/* change these to fit your memory size */
set enable_seqscan = off;
-- set shared_buffers = "25GB";
set effective_cache_size = "50GB";
set work_mem = "25GB";

drop index if exists cmaq_exposures_index;
create index cmaq_exposures_index on cmaq_exposures_data (col, row, date_trunc('day', utc_date_time));


drop table if exists cmaq_daily;
drop index if exists cmaq_daily_index;
create table cmaq_daily as
    select date_trunc('day', utc_date_time) as date, col, row 
    from cmaq_exposures_data 
    group by date, col, row;
create index cmaq_daily_index on cmaq_daily (col, row, date);

drop table if exists cmaq_daily_DES;
drop index if exists cmaq_daily_DES_index;
create table cmaq_daily_DES as 
    select a.col, a.row, a.date, DESpm(max(pmij) :: numeric) as DESpm, DESo(max(o3) :: numeric) as DESo
    from cmaq_daily a inner join cmaq_exposures_data b on b.col = a.col and b.row = a.row and date_trunc('day', b.utc_date_time) = a.date 
    group by a.col, a.row, a.date;
create index cmaq_daily_DES_index on cmaq_daily_DES (col, row, date);

drop table if exists cmaq_7da_DES;
drop index if exists cmaq_7da_DES_index;
create table cmaq_7da_DES as 
    select a.col, a.row, a.date, avg(DESpm) as DESpm_7da, avg(DESo) as DESo_7da  
    from cmaq_daily a inner join cmaq_daily_DES b on b.col = a.col and b.row = a.row and b.date :: timestamp <@ tsrange (a.date :: timestamp - interval '7 day', a.date :: timestamp, '(]') 
    group by a.col, a.row, a.date;
create index cmaq_7da_DES_index on cmaq_7da_DES (col, row, date);

copy cmaq_7da_DES to '/tmp/cmaq_7da_DES.csv' delimiter ',' csv header;

