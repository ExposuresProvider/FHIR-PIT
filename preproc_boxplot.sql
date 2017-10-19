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

drop table if exists start_date_any_18_out;
drop index if exists start_date_any_18_out_index;
drop index if exists start_date_any_18_out_index2;
create table start_date_any_18_out as select start_date as date, encounter_num, patient_num, col, row from start_date_any_18 inner join visit_reduced using (encounter_num, patient_num) inner join rowcol_any_18 using (encounter_num, patient_num) where inout_cd = 'OUTPATIENT';
create index start_date_any_18_out_index on start_date_any_18_out (encounter_num, patient_num);                                                                                                           
create index start_date_any_18_out_index2 on start_date_any_18_out (col, row, date);

drop table if exists start_date_asthma_18_ed;
drop index if exists start_date_asthma_18_ed_index;
drop index if exists start_date_asthma_18_ed_index2;
create table start_date_asthma_18_ed as select a.start_date as date, encounter_num, patient_num, col, row from start_date_any_18 inner join ed_visits_asthma1 a using (encounter_num, patient_num) inner join rowcol_any_18 using (encounter_num, patient_num);
create index start_date_asthma_18_ed_index on start_date_asthma_18_ed (encounter_num, patient_num);                                                                                                           
create index start_date_asthma_18_ed_index2 on start_date_asthma_18_ed (col, row, date);

drop table if exists cmaq_7da_DES_any_18_out;
create table cmaq_7da_DES_any_18_out as 
select 'OUTPATIENT,any' :: text as visit_type, DESpm_7da, DESo_7da from start_date_any_18_out a inner join cmaq_7da_DES b using (col, row, date);

drop table if exists cmaq_7da_DES_asthma_18_ed;
create table cmaq_7da_DES_asthma_18_ed as
select 'ED/INPATIENT,asthma-like' :: text as visit_type, DESpm_7da, DESo_7da from start_date_asthma_18_ed a inner join cmaq_7da_DES b using (col, row, date);

do $$
declare 
total integer;
out_any integer;
ed_asthma integer;
begin
select count(*) into total from cmaq_7da_DES;
select count(*) into out_any from cmaq_7da_DES_any_18_out;
select count(*) into ed_asthma from cmaq_7da_DES_asthma_18_ed;

raise notice 'ratio (% + %) * 100.0 / % = % %%', out_any, ed_asthma, total, (out_any + ed_asthma) * 100.0 / total;
drop table if exists cmaq_7da_DES_random;
create table cmaq_7da_DES_random as
select 'RANDOM' :: text as visit_type, DESpm_7da, DESo_7da from cmaq_7da_DES tablesample bernoulli((out_any + ed_asthma) * 100.0 / total);
end $$;

drop table if exists boxplot;
create table boxplot as (select * from cmaq_7da_DES_any_18_out union select * from cmaq_7da_DES_asthma_18_ed union select * from cmaq_7da_DES_random);

copy boxplot to '/tmp/boxplot.csv' delimiter ',' csv header;


