create or replace function filter_icd(concept_cd text) returns boolean as $$
  begin
    return concept_cd similar to '(ICD9:493|ICD9:464|ICD9:496|ICD9:786|ICD9:481|ICD9:482|ICD9:483|ICD9:484|ICD9:485|ICD9:486|ICD10:345|ICD10:J05|ICD10:J44|ICD10:J66|ICD10:R05|ICD10:J12|ICD10:J13|ICD10:J14|ICD10:J15|ICD10:J16|ICD10:J17|ICD10:J18)%';
  end;
$$ language plpgsql;

create or replace function pm25_table(nds integer) returns void as $$
declare
  table_name text;
  index_name text;
  sql text;
begin
  table_name := 'cmaq_4336094_' || nds || 'da';
  index_name := table_name || '_index';
  execute format('drop table if exists %I', table_name);
  execute format('drop index if exists %I', index_name);
  sql := format(E'create table %I as select a.date as start_date, (select avg(pm25_total_ugm3) from cmaq_4336094 b where b.date :: timestamp <@ tsrange (a.date :: timestamp - interval \'%s day\', a.date :: timestamp, \'[]\')) as pm25_%sda from cmaq_4336094 a where a.date = date_trunc(\'day\', a.date)', table_name, nds, nds);
  raise notice 'sql=%', sql;
  execute sql;
  execute format('create index %I on %I (start_date asc)', index_name, table_name);
end;
$$ language plpgsql;

create type pair_text as (fst text, snd text);

create or replace function observation_table(table_name text, column_name_pairs pair_text[], pat text, dist boolean) returns void as $$
declare
  index_name text;
  sql text;
  column_name_pair pair_text;
begin
  index_name := table_name || '_index';
  execute format('drop table if exists %I', table_name);
  execute format('drop index if exists %I', index_name);
  sql := format('create table %I as select', table_name);
  if dist then
    sql := sql || ' distinct';
  end if;
  sql := sql || ' encounter_num, patient_num';
  foreach column_name_pair in array column_name_pairs loop
    sql := sql || format(', %I as %I', column_name_pair.fst, column_name_pair.snd);
  end loop;
  sql := sql || format(' from observation_fact inner join visits using (encounter_num, patient_num) where concept_cd similar to %L', pat);
  raise notice 'sql=%', sql;
  execute sql;
  execute format('create index %I on %I (encounter_num, patient_num)', index_name, table_name);
end;
$$ language plpgsql;


drop table if exists patient_reduced;
drop index if exists patient_index;
create table patient_reduced as select patient_num, birth_date, sex_cd as sex_cd, race_cd from patient_dimension where birth_date is not null;
create index patient_index on patient_reduced (patient_num );                                                                                                                                  

drop table if exists visit_reduced;
drop index if exists visit_index;
create table visit_reduced as select patient_num, encounter_num, inout_cd from visit_dimension where inout_cd is not null;
create index visit_index on visit_reduced (encounter_num, patient_num);                                                                                                                       

/* find encounter_num and patient_num and icd_code with asthma like diagnosis */
drop table if exists asthma;
create table asthma as select concept_cd, encounter_num, patient_num from observation_fact where filter_icd(concept_cd);

drop table if exists asthma_encounter;
drop index if exists asthma_encounter_index;
create table asthma_encounter as select distinct encounter_num, patient_num from asthma;
create index asthma_encounter_index on asthma_encounter (encounter_num, patient_num);                                                                                                           

drop table if exists asthma_patient;
drop index if exists asthma_patient_index;
create table asthma_patient as select distinct patient_num from asthma;
create index asthma_patient_index on asthma_patient (patient_num);                                                                                                           

/* filter observation_fact */
drop table if exists observation_reduced;
drop index if exists observation_index;
create table observation_reduced as select start_date, encounter_num, patient_num from observation_fact inner join asthma_encounter using (encounter_num, patient_num);
create index observation_index on observation_reduced (encounter_num, patient_num);                                                                                

drop table if exists start_date;
drop index if exists start_date_index;
drop index if exists start_date_index2;
create table start_date as select min(start_date :: timestamp) as start_date, encounter_num, patient_num from observation_reduced group by encounter_num, patient_num;
create index start_date_index on start_date (encounter_num, patient_num);                                                                                                           
create index start_date_index2 on start_date (start_date asc);

/* should assign lat, long based on encounter_num, but the data does it using patient_num */
-- drop table if exists lat;
-- drop index if exists lat_index;
-- create table lat as select patient_num, nval_num as lat from observation_fact inner join asthma_patient using (patient_num) where concept_cd = 'GEO:LAT';
-- create index lat_index on lat ( patient_num);                                                                                                                                   
/* delete patient_num with 0 or >2 lats */
-- with b as (select patient_num from lat group by patient_num having count(lat) <> 1)
-- delete from lat a where exists (select from b where a.patient_num = b.patient_num); 

-- drop table if exists long;
-- drop index if exists long_index;
-- create table long as select patient_num, nval_num as long from observation_fact inner join asthma_patient using (patient_num) where concept_cd = 'GEO:LONG';
-- create index long_index on long ( patient_num);                                                                                                                                   
/* delete patient_num with 0 or >2 longs */
-- with b as (select patient_num from long group by patient_num having count(long) <> 1)
-- delete from long a where exists (select from b where a.patient_num = b.patient_num);

drop table if exists ed_visits0;
create table ed_visits0 as select * from start_date inner join visit_reduced using (encounter_num, patient_num);  

drop table if exists ed_visits1;
drop index if exists ed_visits_index1;
create table ed_visits1 as select * from ed_visits0 where inout_cd in ('INPATIENT', 'EMERGENCY');
create index ed_visits1_index on ed_visits1 (encounter_num, patient_num);      
create index ed_visits1_index2 on ed_visits1 (patient_num);      

drop table if exists ed_visits;
drop index if exists ed_visits_index;
create table ed_visits as select encounter_num, patient_num, start_date, inout_cd, (select count(distinct encounter_num) from ed_visits1 b where b.patient_num = a.patient_num and b.start_date :: timestamp <@ tsrange (a.start_date :: timestamp - interval '1 year', a.start_date :: timestamp, '[)')) as pre_ed, (select count(distinct encounter_num) from ed_visits1 b where b.patient_num = a.patient_num and b.start_date :: timestamp <@ tsrange (a.start_date :: timestamp, a.start_date :: timestamp + interval '1 year', '(]')) as post_ed from ed_visits0 a;
create index ed_visits_index on ed_visits (encounter_num, patient_num);                

drop table if exists cmaq_4336094;
drop index if exists cmaq_4336094_index;
create table cmaq_4336094 as select date, pm25_total_ugm3 from cmaq where id = 4336094;
create index cmaq_4336094_index on cmaq_4336094 (date asc);

do $$ begin
  for i in 1..7 loop
    perform pm25_table(i);
  end loop;
end $$;

drop table if exists visits;
drop index if exists visits_index;
create table visits as select encounter_num, patient_num from cmaq_4336094 a, start_date b where a.date = b.start_date;
create index visits_index on cmaq_available(encounter_num, patient_num);

drop table if exists icd;
drop index if exists icd_index;
create table icd as select distinct encounter_num, patient_num, concept_cd as icd_code from asthma inner join visits using (encounter_num, patient_num);
create index icd_index on icd ( encounter_num, patient_num);                                                                                                                                   

do $$ begin
  perform observation_table('loinc', array[('concept_cd', 'loinc_concept'), ('nval_num', 'loinc_nval'), ('units_cd', 'loinc_units'), ('start_date', 'loinc_start_date')] :: pair_text[], 'LOINC:%', true);
end $$; 

do $$ begin
  perform observation_table('mdctn', array[('concept_cd', 'mdctn_concept'), ('modifier_cd', 'mdctn_modifier'), ('nval_num', 'mdctn_nval'), ('tval_char', 'mdctn_tval'), ('units_cd', 'mdctn_units'), ('start_date', 'mdctn_start_date'), ('end_date', 'mdctn_end_date')] :: pair_text[], 'MDCTN:%', true);
end $$; 

drop table if exists features;
create table features as select *, extract(day from start_date - birth_date) as age from ed_visits inner join patient_reduced using (patient_num) inner join cmaq_4336094_7da using (start_date) inner join cmaq_4336094_1da using (start_date) inner join cmaq_4336094_2da using (start_date) inner join cmaq_4336094_3da using (start_date) inner join cmaq_4336094_4da using (start_date) inner join cmaq_4336094_5da using (start_date) inner join cmaq_4336094_6da using (start_date);

copy features to '/tmp/endotype3.csv' delimiter ',' csv header;
copy loinc to '/tmp/loinc3.csv' delimiter ',' csv header;
copy mdctn to '/tmp/mdctn3.csv' delimiter ',' csv header;
copy icd to '/tmp/icd3.csv' delimiter ',' csv header;


/* clean up */
drop function fliter_icd(text);
drop function pm25_table(integer);
drop table visit_reduced;
drop table observation_reduced;
drop table ed_visits0;
drop table ed_visits1;
drop table ed_visits;
-- drop table lat;
-- drop table long;
drop table icd;
drop table start_date;
drop table cmaq_available;
drop table patient_reduced;
drop table asthma;
drop table asthma_encounter;
drop table asthma_patient;
drop table cmaq_4336094;
drop table loinc;
drop table mdctn;
do $$ begin
  for i in 1..7 loop
    execute format('drop table cmaq_4336094_%sda', i);
  end loop;
end $$;
