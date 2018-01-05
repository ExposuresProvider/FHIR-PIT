/* require postgres >= 9.6
   Ubuntu Xenial:
   https://launchpad.net/~ubuntugis/+archive/ubuntu/ppa?field.series_filter= */

drop type if exists pair_text CASCADE;
create type pair_text as (fst text, snd text);

drop table if exists observation_fact_reduced;
create table observation_fact_reduced as
  select patient_num, encounter_num, concept_cd, modifier_cd, instance_num, valtype_cd, valueflag_cd, nval_num, tval_char, units_cd, start_date, end_date from observation_fact;

drop index if exists observation_fact_index;
drop index if exists observation_fact_index2;
create index observation_fact_index on observation_fact_reduced (patient_num);
create index observation_fact_index2 on observation_fact_reduced (encounter_num, patient_num);

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
    sql := sql || format(', %s as %I', column_name_pair.fst, column_name_pair.snd);
  end loop;
  sql := sql || format(' from observation_fact_reduced where concept_cd similar to %L', pat);
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
create table visit_reduced as select patient_num, encounter_num, inout_cd, start_date, end_date from visit_dimension where inout_cd is not null;
create index visit_index on visit_reduced (encounter_num, patient_num);                                                                                                                       

/* find encounter_num and patient_num and icd_code with asthma like diagnosis */
/* drop table if exists asthma;
create table asthma as select distinct concept_cd, encounter_num, patient_num, start_date, end_date from observation_fact_reduced where filter_icd(concept_cd); */

/* drop table if exists asthma_encounter;
drop index if exists asthma_encounter_index;
create table asthma_encounter as select distinct encounter_num, patient_num from asthma;
create index asthma_encounter_index on asthma_encounter (encounter_num, patient_num); */                                                                                                        

/*drop table if exists asthma_patient;
drop index if exists asthma_patient_index;
create table asthma_patient as select distinct patient_num from asthma;
create index asthma_patient_index on asthma_patient (patient_num); */                                                                                                            

/* drop table if exists start_end_date_asthma;
drop index if exists start_end_date_asthma_index;
drop index if exists start_end_date_asthma_index2;
drop index if exists start_end_date_asthma_index3;
create table start_end_date_asthma as select start_date, end_date, encounter_num, patient_num from visit_reduced inner join asthma_patient using (patient_num) group by encounter_num, patient_num;
create index start_end_date_asthma_index on start_end_date_asthma (encounter_num, patient_num);                                                                                                           
create index start_end_date_asthma_index2 on start_end_date_asthma (start_date asc);
create index start_end_date_asthma_index3 on start_end_date_asthma (end_date asc); */

/* filter observation_fact */
/*drop table if exists observation_asthma_reduced;
drop index if exists observation_asthma_index;
create table observation_asthma_reduced as select start_date, encounter_num, patient_num from observation_fact inner join asthma_encounter using (encounter_num, patient_num);
create index observation_asthma_index on observation_asthma_reduced (encounter_num, patient_num);                                                                                

drop table if exists start_date_asthma;
drop index if exists start_date_asthma_index;
drop index if exists start_date_asthma_index2;
create table start_date_asthma as select min(start_date :: timestamp) as start_date, encounter_num, patient_num from observation_asthma_reduced group by encounter_num, patient_num;
create index start_date_asthma_index on start_date_asthma (encounter_num, patient_num);                                                                                                           
create index start_date_asthma_index2 on start_date_asthma (start_date asc); */

/* should assign lat, long based on encounter_num, but the data does it using patient_num */
drop table if exists lat;
drop index if exists lat_index;
create table lat as select patient_num, nval_num as lat from observation_fact_reduced /* inner join asthma_patient using (patient_num) */ where concept_cd like 'GEO:LAT';
create index lat_index on lat ( patient_num);                                                                                                                                   
/* delete patient_num with 0 or >2 lats */
with b as (select patient_num from lat group by patient_num having count(lat) <> 1)
delete from lat a where exists (select from b where a.patient_num = b.patient_num); 

drop table if exists long;
drop index if exists long_index;
create table long as select patient_num, nval_num as long from observation_fact_reduced /* inner join asthma_patient using (patient_num) */ where concept_cd like 'GEO:LONG';
create index long_index on long ( patient_num);                                                                                                                                   
/* delete patient_num with 0 or >2 longs */
with b as (select patient_num from long group by patient_num having count(long) <> 1)
delete from long a where exists (select from b where a.patient_num = b.patient_num);

drop table if exists latlong;
drop index if exists latlong_index;
create table latlong as select * from lat inner join long using (patient_num);
create index latlong_index on latlong ( patient_num);       

/* drop table if exists ed_visits_asthma0;
create table ed_visits_asthma0 as select asthma.start_date astham_start_date, visit_reduced.start_date start_date, inout_cd, patient_num, encounter_num from asthma inner join visit_reduced using (encounter_num, patient_num);  

drop table if exists ed_visits_asthma1;
drop index if exists ed_visits_asthma1_index;
drop index if exists ed_visits_asthma1_index2;
create table ed_visits_asthma1 as select * from ed_visits_asthma0 where inout_cd in ('INPATIENT', 'EMERGENCY');
create index ed_visits_asthma1_index on ed_visits_asthma1 (encounter_num, patient_num);      
create index ed_visits_asthma1_index2 on ed_visits_asthma1 (patient_num);      

drop table if exists ed_visits_asthma;
drop index if exists ed_visits_asthma_index;
create table ed_visits_asthma as
  select encounter_num, patient_num, start_date, inout_cd,
    (select count(distinct encounter_num) from ed_visits_asthma1 b where b.patient_num = a.patient_num and b.start_date :: timestamp <@ tsrange (a.start_date :: timestamp - interval '1 year', a.start_date :: timestamp, '[)')) as pre_ed,
    (select count(distinct encounter_num) from ed_visits_asthma1 b where b.patient_num = a.patient_num and b.start_date :: timestamp <@ tsrange (a.start_date :: timestamp, a.start_date :: timestamp + interval '1 year', '(]')) as post_ed
  from ed_visits_asthma0 a;
create index ed_visits_asthma_index on ed_visits_asthma (encounter_num, patient_num); */               

do $$ begin
  perform observation_table('loinc', array[('(concept_cd || ''_'' || (instance_num :: text))', 'concept'), ('valtype_cd', 'valtype'), ('nval_num', 'nval'), 
         ('valueflag_cd', 'valueflag'), ('tval_char', 'tval'), ('units_cd', 'units'), ('start_date', 'start_date'), 
         ('end_date', 'end_date')] :: pair_text[], 'LOINC:%', true);
end $$; 

do $$ begin
  perform observation_table('mdctn', array[('(concept_cd || ''_'' || modifier_cd || ''_'' || (instance_num :: text))', 'concept'), ('valtype_cd', 'valtype'),
         ('valueflag_cd', 'valueflag'), ('nval_num', 'nval'), ('tval_char', 'tval'), ('units_cd', 'units'), 
         ('start_date', 'start_date'), ('end_date', 'end_date')] :: pair_text[], 'MDCTN:%', true);
end $$;

do $$ begin
  perform observation_table('vital', array[('(concept_cd || ''_'' || (instance_num :: text))', 'concept'), ('valtype_cd', 'valtype'), 
         ('valueflag_cd', 'valueflag'), ('nval_num', 'nval'), ('tval_char', 'tval'), 
         ('units_cd', 'units'), ('start_date', 'start_date'), ('end_date', 'end_date')] :: pair_text[], 'VITAL:%', true);
end $$;

/*
do $$ begin
  perform observation_table('cpt', array[('concept_cd', 'concept'), ('valtype_cd', 'valtype'), 
         ('valueflag_cd', 'valueflag'), ('nval_num', 'nval'), ('tval_char', 'tval'), ('units_cd', 'units'), 
         ('start_date', 'start_date'), ('end_date', 'end_date')] :: pair_text[], 'CPT:%', true);
end $$;

do $$ begin
  perform observation_table('soc_hist', array[('concept_cd', 'concept'), ('valtype_cd', 'valtype'), 
         ('valueflag_cd', 'valueflag'), ('nval_num', 'nval'), ('tval_char', 'val'), ('units_cd', 'units'), 
         ('start_date', 'start_date'), ('end_date', 'end_date')] :: pair_text[], 'SOC_HIST:%', true);
end $$;
*/

do $$ begin
  perform observation_table('icd', array[('concept_cd', 'icd_code'), 
         ('start_date', 'start_date'), ('end_date', 'end_date')] :: pair_text[], 'ICD%', true);
end $$;

drop table if exists features;
create table features as select */* , extract(day from start_date - birth_date) as age*/ from patient_reduced inner join latlong using (patient_num);

drop table if exists icd_norm;
create table icd_norm as select encounter_num, patient_num, start_date, end_date, icd_code, True norm from icd;

-- long to wide


select longtowide('icd_norm', ARRAY['encounter_num','patient_num'], ARRAY['integer','integer'], 'icd_code',
                  ARRAY['norm','start_date','end_date'],
		  ARRAY['boolean','timestamp','timestamp'],
		  ARRAY['icd_code','icd_start_date','icd_end_data'], 'icd_norm_wide');
select longtowide('loinc', ARRAY['encounter_num','patient_num'], ARRAY['integer','integer'], 'concept',
                  ARRAY['valtype','nval','tval','units','start_date','end_date','valueflag'],
                  ARRAY['varchar(50)', 'numeric', 'varchar(255)','varchar(50)', 'timestamp', 'timestamp','varchar(50)'],
		  ARRAY['loinc_valtype','loinc_nval','loinc_tval','loinc_units','loinc_start_date','loinc_end_date','loinc_valueflag'], 'loinc_wide');
select longtowide('mdctn', ARRAY['encounter_num','patient_num'], ARRAY['integer','integer'], 'concept',
                  ARRAY['valtype','nval','tval','units','start_date','end_date','valueflag'],
                  ARRAY['varchar(50)', 'numeric', 'varchar(255)','varchar(50)', 'timestamp', 'timestamp','varchar(50)'],
                  ARRAY['mdctn_valtype','mdctn_nval','mdctn_tval','mdctn_units','mdctn_start_date','mdctn_end_date','mdctn_valueflag'], 'mdctn_wide');
select longtowide('vital', ARRAY['encounter_num','patient_num'], ARRAY['integer','integer'], 'concept',
                  ARRAY['valtype','nval','tval','units','start_date','end_date','valueflag'],
                  ARRAY['varchar(50)', 'numeric', 'varchar(255)','varchar(50)', 'timestamp', 'timestamp','varchar(50)'],
                  ARRAY['vital_valtype','vital_nval','vital_tval','vital_units','vital_start_date','vital_end_date','vital_valueflag'], 'vital_wide');


drop table if exists features_wide;
create table features_wide as
  select *
  from visit_reduced
    full outer join icd_norm_wide using (patient_num, encounter_num)
    full outer join loinc_wide using (patient_num, encounter_num)
    full outer join mdctn_wide using (patient_num, encounter_num)
    full outer join vital_wide using (patient_num, encounter_num)
    inner join features using (patient_num);

create temp table tmp (like features_wide);
copy tmp to '/tmp/endotype_meta.csv' delimiter '!' csv header;
drop table tmp;
copy features_wide to '/tmp/endotype.csv' delimiter '!' null '';
copy loinc_wide_meta to '/tmp/loinc_meta.csv' delimiter '!';
copy mdctn_wide_meta to '/tmp/mdctn_meta.csv' delimiter '!';
copy vital_wide_meta to '/tmp/vital_meta.csv' delimiter '!';
copy icd_norm_wide_meta to '/tmp/icd_meta.csv' delimiter '!';


drop index if exists mdctn_code_to_mdctn_name_index;
create index mdctn_code_to_mdctn_name_index on mdctn_code_to_mdctn_name(concept_cd);

drop table if exists mdctn_rxnorm;
create table mdctn_rxnorm as
  select patient_num,
         encounter_num,
         (rxnorm || '_' || modifier_cd || '_' || instance_num) as concept,
         valtype_cd as valtype,
         valueflag_cd as valueflag,
         nval_num as nval,
         tval_char as tval,
         units_cd as units,
         start_date,
         end_date
         from observation_fact inner join mdctn_code_to_mdctn_name using (concept_cd) inner join mdctn_name_to_rxnorm using (mdctn_name);
											      
drop table if exists mdctn_gene_norm;
create table mdctn_gene_norm as
  select patient_num,
         encounter_num,
         gene as concept,
	 true as norm
         from observation_fact inner join mdctn_code_to_mdctn_name using (concept_cd) inner join mdctn_name_to_gene using (mdctn_name);

create or replace function filter_icd(concept_cd text) returns boolean as $$
  begin
    return concept_cd similar to '(ICD9:493.%|ICD10:J45.%|ICD9:464.%|ICD10:J05.%|ICD9:496.%|ICD10:J44.%|ICD10:J66.%|ICD9:786.%|ICD10:R05.%|ICD9:481.%|ICD9:482.%|ICD9:483.%|ICD9:484.%|ICD9:485.%|ICD9:486.%|ICD10:J12.%|ICD10:J13.%|ICD10:J14.%|ICD10:J15.%|ICD10:J16.%|ICD10:J17.%|ICD10:J18.%|ICD9:278.00|ICD10:E66.%)' and concept_cd not like 'ICD10:E66.3';
  end;
$$ language plpgsql;

create or replace function filter_loinc(concept_cd text) returns boolean as $$
  begin
    return concept_cd similar to 'LOINC:(33536-4|13834-7|26449-9|711-2|712-0|26450-7|713-8|714-6|26499-4|751-8|753-4|26511-6|770-8|23761-0|1988-5|30522-7|11039-5|35648-5|76485-2|76486-0|14634-0|71426-1)\_%';
  end;
$$ language plpgsql;

create or replace function truncate_icd(concept_cd text) returns text as $$
  begin
    return split_part(concept_cd, '.', 1);
  end;
$$ language plpgsql;

-- truncate & filter icd
drop table if exists icd_filter_trunc_norm;
create table icd_filter_trunc_norm as select distinct encounter_num, patient_num, start_date, end_date, truncate_icd(icd_code) as icd_code, norm from icd_norm where filter_icd(icd_code);

-- filter loinc
drop table if exists loinc_filter;
create table loinc_filter as select * from loinc where filter_loinc(concept);

-- long to wide
select longtowide('loinc_filter', ARRAY['encounter_num','patient_num'], ARRAY['integer','integer'], 'concept',
                  ARRAY['valtype','nval','tval','units','start_date','end_date','valueflag'],
		  ARRAY['varchar(50)', 'numeric', 'varchar(255)', 'varchar(50)', 'timestamp', 'timestamp','varchar(50)'],
		  ARRAY['loinc_valtype','loinc_nval','loinc_tval','loinc_units','loinc_start_date','loinc_end_date','loinc_valueflag'], 'loinc_filter_wide');
select longtowide('icd_filter_trunc_norm', ARRAY['encounter_num','patient_num'], ARRAY['integer','integer'], 'icd_code',
                  ARRAY['norm','start_date','end_date'],
		  ARRAY['boolean','timestamp','timestamp'],
		  ARRAY['icd_code','icd_start_date','icd_end_date'], 'icd_filter_trunc_norm_wide');
select longtowide('mdctn_rxnorm', ARRAY['encounter_num','patient_num'], ARRAY['integer','integer'], 'concept',
                  ARRAY['valtype','nval','tval','units','start_date','end_date','valueflag'],
                  ARRAY['varchar(50)', 'numeric', 'varchar(255)','varchar(50)', 'timestamp', 'timestamp','varchar(50)'],
                  ARRAY['mdctn_valtype','mdctn_nval','mdctn_tval','mdctn_units','mdctn_start_date','mdctn_end_date','mdctn_valueflag'], 'mdctn_rxnorm_norm_wide');
select longtowide('mdctn_gene_norm', ARRAY['encounter_num','patient_num'], ARRAY['integer','integer'], 'concept',
                  ARRAY['norm'],
                  ARRAY['boolean'],
                  ARRAY['gene'], 'mdctn_gene_norm_wide');
select longtowide('vital', ARRAY['encounter_num','patient_num'], ARRAY['integer','integer'], 'concept',
                  ARRAY['valtype','nval','tval','units','start_date','end_date','valueflag'],
                  ARRAY['varchar(50)', 'numeric', 'varchar(255)','varchar(50)', 'timestamp', 'timestamp','varchar(50)'],
                  ARRAY['vital_valtype','vital_nval','vital_tval','vital_units','vital_start_date','vital_end_date','vital_valueflag'], 'vital_wide');


drop table if exists features_filter_trunc_wide;
create table features_filter_trunc_wide as
  select *
  from visit_reduced
    full outer join icd_filter_trunc_norm_wide using (patient_num, encounter_num)
    full outer join loinc_filter_wide using (patient_num, encounter_num)
    full outer join mdctn_rxnorm_norm_wide using (patient_num, encounter_num)
    full outer join mdctn_gene_norm_wide using (patient_num, encounter_num)
    full outer join vital_wide using (patient_num, encounter_num)
    inner join features using (patient_num);

create temp table tmp (like features_filter_trunc_wide);
copy tmp to '/tmp/endotype_filter_trunc_meta.csv' delimiter '!' csv header;
drop table tmp;
copy features_filter_trunc_wide to '/tmp/endotype_filter_trunc.csv' delimiter '!' null '';
copy loinc_filter_wide_meta to '/tmp/loinc_filter_trunc_meta.csv' delimiter '!';
copy mdctn_rxnorm_norm_wide_meta to '/tmp/mdctn_rxnorm_filter_trunc_meta.csv' delimiter '!';
copy mdctn_gene_norm_wide_meta to '/tmp/mdctn_gene_filter_trunc_meta.csv' delimiter '!';
copy vital_wide_meta to '/tmp/vital_filter_trunc_meta.csv' delimiter '!';
copy icd_filter_trunc_norm_wide_meta to '/tmp/icd_filter_trunc_meta.csv' delimiter '!';





