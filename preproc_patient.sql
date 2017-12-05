/* require postgres >= 9.6
   Ubuntu Xenial:
   https://launchpad.net/~ubuntugis/+archive/ubuntu/ppa?field.series_filter= */

create or replace function filter_icd(concept_cd text) returns boolean as $$
  begin
    return concept_cd similar to '(ICD9:493|ICD9:464|ICD9:496|ICD9:786|ICD9:481|ICD9:482|ICD9:483|ICD9:484|ICD9:485|ICD9:486|ICD10:J45|ICD10:J05|ICD10:J44|ICD10:J66|ICD10:R05|ICD10:J12|ICD10:J13|ICD10:J14|ICD10:J15|ICD10:J16|ICD10:J17|ICD10:J18)%';
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
  sql := sql || format(' from observation_fact where concept_cd similar to %L', pat);
  raise notice 'sql=%', sql;
  execute sql;
  execute format('create index %I on %I (encounter_num, patient_num)', index_name, table_name);
end;
$$ language plpgsql;


drop table if exists patient_reduced;
drop index if exists patient_index;
create table patient_reduced as select patient_num, birth_date, sex_cd, race_cd from patient_dimension where birth_date is not null;
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
drop table if exists observation_asthma_reduced;
drop index if exists observation_asthma_index;
create table observation_asthma_reduced as select start_date, encounter_num, patient_num from observation_fact inner join asthma_encounter using (encounter_num, patient_num);
create index observation_asthma_index on observation_asthma_reduced (encounter_num, patient_num);                                                                                

drop table if exists start_date_asthma;
drop index if exists start_date_asthma_index;
drop index if exists start_date_asthma_index2;
create table start_date_asthma as select min(start_date :: timestamp) as start_date, encounter_num, patient_num from observation_asthma_reduced group by encounter_num, patient_num;
create index start_date_asthma_index on start_date_asthma (encounter_num, patient_num);                                                                                                           
create index start_date_asthma_index2 on start_date_asthma (start_date asc);

/* should assign lat, long based on encounter_num, but the data does it using patient_num */
drop table if exists lat_asthma;
drop index if exists lat_asthma_index;
create table lat_asthma as select patient_num, nval_num as lat from observation_fact inner join asthma_patient using (patient_num) where concept_cd = 'GEO:LAT';
create index lat_asthma_index on lat_asthma ( patient_num);                                                                                                                                   
/* delete patient_num with 0 or >2 lats */
with b as (select patient_num from lat_asthma group by patient_num having count(lat) <> 1)
delete from lat_asthma a where exists (select from b where a.patient_num = b.patient_num); 

drop table if exists long_asthma;
drop index if exists long_asthma_index;
create table long_asthma as select patient_num, nval_num as long from observation_fact inner join asthma_patient using (patient_num) where concept_cd = 'GEO:LONG';
create index long_asthma_index on long_asthma ( patient_num);                                                                                                                                   
/* delete patient_num with 0 or >2 longs */
with b as (select patient_num from long_asthma group by patient_num having count(long) <> 1)
delete from long_asthma a where exists (select from b where a.patient_num = b.patient_num);

drop table if exists latlong_asthma;
drop index if exists latlong_asthma_index;
create table latlong_asthma as select * from lat_asthma inner join long_asthma using (patient_num);
create index latlong_asthma_index on latlong_asthma ( patient_num);       

drop table if exists ed_visits_asthma0;
create table ed_visits_asthma0 as select * from start_date_asthma inner join visit_reduced using (encounter_num, patient_num);  

drop table if exists ed_visits_asthma1;
drop index if exists ed_visits_asthma1_index;
drop index if exists ed_visits_asthma1_index2;
create table ed_visits_asthma1 as select * from ed_visits_asthma0 where inout_cd in ('INPATIENT', 'EMERGENCY');
create index ed_visits_asthma1_index on ed_visits_asthma1 (encounter_num, patient_num);      
create index ed_visits_asthma1_index2 on ed_visits_asthma1 (patient_num);      

drop table if exists ed_visits_asthma;
drop index if exists ed_visits_asthma_index;
create table ed_visits_asthma as select encounter_num, patient_num, start_date, inout_cd, (select count(distinct encounter_num) from ed_visits_asthma1 b where b.patient_num = a.patient_num and b.start_date :: timestamp <@ tsrange (a.start_date :: timestamp - interval '1 year', a.start_date :: timestamp, '[)')) as pre_ed, (select count(distinct encounter_num) from ed_visits_asthma1 b where b.patient_num = a.patient_num and b.start_date :: timestamp <@ tsrange (a.start_date :: timestamp, a.start_date :: timestamp + interval '1 year', '(]')) as post_ed from ed_visits_asthma0 a;
create index ed_visits_asthma_index on ed_visits_asthma (encounter_num, patient_num);                

drop table if exists icd_asthma;
drop index if exists icd_asthma_index;
create table icd_asthma as select distinct encounter_num, patient_num, concept_cd as icd_code from asthma inner join latlong_asthma using (patient_num) where filter_icd(concept_cd);
create index icd_asthma_index on icd_asthma ( encounter_num, patient_num);                                                                                                                                   

do $$ begin
  perform observation_table('loinc', array[('concept_cd', 'concept'), ('valtype_cd', 'valtype'), ('nval_num', 'nval'), 
         ('instance_num', 'instance_num'), ('tval_char', 'tval'), ('units_cd', 'units'), ('start_date', 'start_date'), 
         ('end_date', 'end_date')] :: pair_text[], 'LOINC:%', true);
end $$; 

do $$ begin
  perform observation_table('mdctn', array[('concept_cd', 'concept'), ('modifier_cd', 'modifier'), ('valtype_cd', 'valtype'), ('instance_num', 'instance_num'),
         ('valueflag_cd', 'valueflag'), ('nval_num', 'nval'), ('tval_char', 'tval'), ('units_cd', 'units'), 
         ('start_date', 'start_date'), ('end_date', 'end_date')] :: pair_text[], 'MDCTN:%', true);
end $$;

/*
do $$ begin
  perform observation_table('vital', array[('concept_cd', 'concept'), ('modifier_cd', 'modifier'), ('valtype_cd', 'valtype'), 
         ('instance_num', 'instance_num'), ('valueflag_cd', 'valueflag'), ('nval_num', 'nval'), ('tval_char', 'tval'), 
         ('units_cd', 'units'), ('start_date', 'start_date'), ('end_date', 'end_date')] :: pair_text[], 'VITAL:%', true);
end $$;

do $$ begin
  perform observation_table('cpt', array[('concept_cd', 'oncept'), ('modifier_cd', 'modifier'), ('valtype_cd', 'valtype'), 
         ('valueflag_cd', 'valueflag'), ('nval_num', 'nval'), ('tval_char', 'tval'), ('units_cd', 'units'), 
         ('start_date', 'start_date'), ('end_date', 'end_date')] :: pair_text[], 'CPT:%', true);
end $$;

do $$ begin
  perform observation_table('soc_hist', array[('concept_cd', 'concept'), ('modifier_cd', 'modifier'), ('valtype_cd', 'valtype'), 
         ('valueflag_cd', 'valueflag'), ('nval_num', 'nval'), ('tval_char', 'val'), ('units_cd', 'units'), 
         ('start_date', 'start_date'), ('end_date', 'end_date')] :: pair_text[], 'SOC_HIST:%', true);
end $$;
*/

drop table if exists features;
create table features as select *, extract(day from start_date - birth_date) as age from ed_visits_asthma inner join patient_reduced using (patient_num) inner join latlong_asthma using (patient_num);

-- long to wide

drop table if exists icd_asthma_norm;
create table icd_asthma_norm as select *, True norm from icd_asthma;

select longtowide('icd_asthma_norm', ARRAY['encounter_num','patient_num'], ARRAY['integer','integer'], 'icd_code', ARRAY['norm'], ARRAY['boolean'], ARRAY['icd_code'], 'icd_asthma_norm_wide');
select longtowide('loinc', ARRAY['encounter_num','patient_num'], ARRAY['integer','integer'], 'concept',
                  ARRAY['valtype','instance_num','nval','tval','units','start_date','end_date'],
		  ARRAY['varchar(50)', 'integer', 'numeric', 'varchar(255)', 'varchar(50)', 'timestamp', 'timestamp'],
		  ARRAY['loinc_valtype','loinc_instance_num','loinc_nval','loinc_tval','loinc_units','loinc_start_date','loinc_end_date'], 'loinc_wide');
select longtowide('mdctn', ARRAY['encounter_num','patient_num'], ARRAY['integer','integer'], 'concept',
                  ARRAY['valtype','instance_num','nval','tval','units','start_date','end_date','modifier','valueflag'],
                  ARRAY['varchar(50)', 'integer', 'numeric', 'varchar(255)','varchar(50)', 'timestamp', 'timestamp','varchar(100)','varchar(50)'],
                  ARRAY['mdctn_valtype','mdctn_instance_num','mdctn_nval','mdctn_tval','mdctn_units','mdctn_start_date','mdctn_end_date','mdctn_modifier','mdctn_valueflag'], 'mdctn_wide');


create table features_wide as select * from features inner join icd_asthma_norm_wide using (patient_num, encounter_num) inner join loinc_wide using (patient_num, encounter_num) inner join mdctn_wide using (patient_num, encounter_num);
		  
copy features_wide to '/tmp/endotype3.csv' delimiter ',' csv header;
-- copy loinc_wide to '/tmp/loinc3.csv' delimiter ',' csv header;
-- copy mdctn_wide to '/tmp/mdctn3.csv' delimiter ',' csv header;
-- copy vital_wide to '/tmp/vital3.csv' delimiter ',' csv header;
-- copy cpt_wide to '/tmp/cpt3.csv' delimiter ',' csv header;
-- copy soc_hist_wide to '/tmp/soc_hist3.csv' delimiter ',' csv header;
-- copy icd_asthma_norm_wide to '/tmp/icd3.csv' delimiter ',' csv header;
copy loinc_wide_meta to '/tmp/loinc_meta3.csv' delimiter ',' csv header;
copy mdctn_wide_meta to '/tmp/mdctn_meta3.csv' delimiter ',' csv header;
-- copy vital_wide_meta to '/tmp/vital_meta3.csv' delimiter ',' csv header;
-- copy cpt_wide_meta to '/tmp/cpt_meta3.csv' delimiter ',' csv header;
-- copy soc_hist_wide_meta to '/tmp/soc_hist_meta3.csv' delimiter ',' csv header;
copy icd_asthma_norm_wide_meta to '/tmp/icd_meta3.csv' delimiter ',' csv header;


