drop table if exists start_date_any;
drop index if exists start_date_any_index;
drop index if exists start_date_any_index2;
create table start_date_any as select min(start_date :: timestamp) as start_date, encounter_num, patient_num from observation_fact group by encounter_num, patient_num;
create index start_date_any_index on start_date_any (encounter_num, patient_num);                                                                                                           
create index start_date_any_index2 on start_date_any (start_date asc);

drop table if exists start_date_any_18;
drop index if exists start_date_any_18_index;
drop index if exists start_date_any_18_index2;
create table start_date_any_18 as select start_date, encounter_num, patient_num from start_date_any inner join patient_reduced using (patient_num) where start_date :: timestamp - birth_date :: timestamp < interval '18 years';
create index start_date_any_18_index on start_date_any_18 (encounter_num, patient_num);                                                                                                           
create index start_date_any_18_index2 on start_date_any_18 (start_date asc);

drop table if exists patient_any_18;
drop index if exists patient_any_18_index;
create table patient_any_18 as select distinct patient_num from start_date_any_18;
create index patient_any_18_index on patient_any_18 (patient_num);                                                                                                           

drop table if exists lat_any_18;
drop index if exists lat_any_18_index;
create table lat_any_18 as select patient_num, nval_num as lat from observation_fact inner join patient_any_18 using (patient_num) where concept_cd = 'GEO:LAT';
create index lat_any_18_index on lat_any_18 ( patient_num);                                                                                                                                   
/* delete patient_num with 0 or >2 lats */
with b as (select patient_num from lat_any_18 group by patient_num having count(lat) <> 1)
delete from lat_any_18 a where exists (select from b where a.patient_num = b.patient_num); 

drop table if exists long_any_18;
drop index if exists long_any_18_index;
create table long_any_18 as select patient_num, nval_num as long from observation_fact inner join patient_any_18 using (patient_num) where concept_cd = 'GEO:LONG';
create index long_any_18_index on long_any_18 ( patient_num);                                                                                                                                   
/* delete patient_num with 0 or >2 longs */
with b as (select patient_num from long_any_18 group by patient_num having count(long) <> 1)
delete from long_any_18 a where exists (select from b where a.patient_num = b.patient_num);

drop table if exists latlong_any_18;
drop index if exists latlong_any_18_index;
create table latlong_any_18 as select * from lat_any_18 inner join long_any_18 using (patient_num);
create index latlong_any_18_index on latlong_any_18 ( patient_num);       

drop table if exists rowcol_any_18;
drop index if exists rowcol_any_18_index;
drop index if exists rowcol_any_18_index2;
create table rowcol_any_18 as 
    select encounter_num, patient_num, (a.coors).row as row, (a.coors).col as col, date 
    from (
        select encounter_num, patient_num, latlon2rowcol(lat, long, date_part('year', start_date) :: integer) as coors, date_trunc('day', start_date) as date 
        from latlong_any_18 inner join start_date_any_18 using (patient_num)) as a
    where (a.coors).row <> -1 and (a.coors).col <> -1;
create index rowcol_any_18_index on rowcol_any_18 (encounter_num, patient_num);      
create index rowcol_any_18_index2 on rowcol_any_18 (col, row, date);      

drop table if exists col_row_date_any_18_out;
drop index if exists col_row_date_any_18_out_index;
drop index if exists col_row_date_any_18_out_index2;
create table col_row_date_any_18_out as select start_date as date, encounter_num, patient_num, col, row from start_date_any_18 inner join visit_reduced using (encounter_num, patient_num) inner join rowcol_any_18 using (encounter_num, patient_num) where inout_cd = 'OUTPATIENT';
create index col_row_date_any_18_out_index on col_row_date_any_18_out (encounter_num, patient_num);                                                                                                           
create index col_row_date_any_18_out_index2 on col_row_date_any_18_out (col, row, date);

drop table if exists col_row_date_asthma_18_ed;
drop index if exists col_row_date_asthma_18_ed_index;
drop index if exists col_row_date_asthma_18_ed_index2;
create table col_row_date_asthma_18_ed as select a.start_date as date, encounter_num, patient_num, col, row from start_date_any_18 inner join ed_visits_asthma1 a using (encounter_num, patient_num) inner join rowcol_any_18 using (encounter_num, patient_num);
create index col_row_date_asthma_18_ed_index on col_row_date_asthma_18_ed (encounter_num, patient_num);                                                                                                           
create index col_row_date_asthma_18_ed_index2 on col_row_date_asthma_18_ed (col, row, date);

create or replace function pm25_table(total integer, nds integer) returns void as $$
declare
  table_name_any_out text;
  table_name_asthma_ed text;
  table_name_random text;
  table_name_bp text;
  sql_any_out text;
  sql_asthma_ed text;
  sql_random text;
  sql_bp text;
  i interval;
  out_any integer;
  ed_asthma integer;
  sample_rate numeric;
begin
  i := (nds || ' day') :: interval;

  table_name_any_out := 'cmaq_7da_daily_any_18_out_' || nds;
  execute format('drop table if exists %I', table_name_any_out);
  sql_any_out := format(E'create table %I as select \'OUTPATIENT,any\' :: text as visit_type, DESpm, DESo from col_row_date_any_18_out a inner join cmaq_daily_DES b on b.col = a.col and b.row = a.row and b.date + %L = a.date', table_name_any_out, i);
  raise notice 'sql=%', sql_any_out;
  execute sql_any_out;

  table_name_asthma_ed := 'cmaq_7da_daily_asthma_18_ed_' || nds;
  execute format('drop table if exists %I', table_name_asthma_ed);
  sql_asthma_ed := format(E'create table %I as select \'ED/INPATIENT,asthma-like\' :: text as visit_type, DESpm, DESo from col_row_date_asthma_18_ed a inner join cmaq_daily_DES b on b.col = a.col and b.row = a.row and b.date + %L = a.date', table_name_asthma_ed, i);
  raise notice 'sql_2=%', sql_asthma_ed;
  execute sql_asthma_ed;

  execute format('select count(*) from %I', table_name_any_out) into out_any;
  execute format('select count(*) from %I', table_name_asthma_ed) into ed_asthma;

  sample_rate := (out_any + ed_asthma) * 100.0 / total;
  raise notice 'ratio (% + %) * 100.0 / % = % %%', out_any, ed_asthma, total, sample_rate;

  table_name_random := 'cmaq_7da_DES_random_' || nds;
  execute format(E'drop table if exists %I', table_name_random);
  sql_random := format(E'create table %I as select \'RANDOM\' :: text as visit_type, DESpm, DESo from cmaq_daily_DES tablesample bernoulli(%L)', table_name_random, sample_rate);
  raise notice 'sql_3=%', sql_random;
  execute sql_random;

  table_name_bp := 'boxplot_' || nds;
  execute format('drop table if exists %I', table_name_bp);
  sql_bp := format('create table %I as (select * from %I union all select * from %I union all select * from %I)', table_name_bp, table_name_any_out, table_name_asthma_ed, table_name_random);
  raise notice 'sql_3=%', sql_bp;
  execute sql_bp;

  execute format(E'copy %I to %L delimiter \',\' csv header', table_name_bp, '/tmp/' || table_name_bp || '.csv');
end;
$$ language plpgsql;

do $$ 
declare
total integer;
begin
  select count(*) into total from cmaq_7da_DES;

  for i in 0..6 loop
    perform pm25_table(total, i);
  end loop;
end $$;


