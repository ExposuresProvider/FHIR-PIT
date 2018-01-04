/* assuming that we have a table mdctn_code_to_name(concept_cd text, name text) */

drop index if exists mdctn_code_to_name_index;
create index mdctn_code_to_name_index on mdctn_code_to_name(concept_cd);

drop table if exists mdctn_name;
create table mdctn_name as
  select patient_num,
         encounter_num,
	 (name || '_' || instance_num) as concept,
	 modifier_cd as modifier,
	 valtype_cd as valtype,
	 valueflag_cd as valueflag,
	 nval_num as nval,
	 tval_char as tval,
	 units_cd as units,
	 start_date,
	 end_date
  from observation_fact inner join mdctn_code_to_name using (concept_cd);

select longtowide('mdctn_name', ARRAY['encounter_num','patient_num'], ARRAY['integer','integer'], 'concept',
                  ARRAY['valtype','nval','tval','units','start_date','end_date','modifier','valueflag'],
		  ARRAY['varchar(50)', 'numeric', 'varchar(255)','varchar(50)', 'timestamp', 'timestamp','varchar(100)','varchar(50)'],
		  ARRAY['mdctn_valtype','mdctn_nval','mdctn_tval','mdctn_units','mdctn_start_date','mdctn_end_date','mdctn_modifier','mdctn_valueflag'], 'mdctn_name_wide');
						      
drop table if exists features_mdctn_name_wide;
create table features_mdctn_name_wide as
  select *
  from visit_reduced
  full outer join icd_norm_wide using (patient_num, encounter_num)
  full outer join loinc_wide using (patient_num, encounter_num)
  full outer join mdctn_name_wide using (patient_num, encounter_num)
  full outer join vital_wide using (patient_num, encounter_num)
  inner join features using (patient_num);

create temp table tmp (like features_mdctn_name_wide);
copy tmp to '/tmp/endotype_meta.csv' delimiter '!' csv header;
drop table tmp;
copy features_mdctn_name_wide to '/tmp/endotype_mdctn_name.csv' delimiter '!' null '';
copy loinc_wide_meta to '/tmp/loinc_mdctn_name_meta.csv' delimiter '!';
copy mdctn_name_wide_meta to '/tmp/mdctn_mdctn_name_meta.csv' delimiter '!';
copy vital_wide_meta to '/tmp/vital_mdctn_name_meta.csv' delimiter '!';
copy icd_norm_wide_meta to '/tmp/icd_mdctn_name_meta.csv' delimiter '!';

drop table if exists features_filter_trunc_mdctn_name_wide;
create table features_filter_trunc_mdctn_name_wide as
  select *
  from visit_reduced
  full outer join icd_filter_trunc_norm_wide using (patient_num, encounter_num)
  full outer join loinc_filter_wide using (patient_num, encounter_num)
  full outer join mdctn_name_wide using (patient_num, encounter_num)
  full outer join vital_wide using (patient_num, encounter_num)
  inner join features using (patient_num);

create temp table tmp (like features_filter_trunc_mdctn_name_wide);
copy tmp to '/tmp/endotype_filter_trunc_mdctn_name_meta.csv' delimiter '!' csv header;
drop table tmp;
copy features_filter_trunc_mdctn_name_wide to '/tmp/endotype_filter_trunc_mdctn_name.csv' delimiter '!' null '';
copy loinc_filter_wide_meta to '/tmp/loinc_filter_trunc_mdctn_name_meta.csv' delimiter '!';
copy mdctn_name_wide_meta to '/tmp/mdctn_filter_trunc_mdctn_name_meta.csv' delimiter '!';
copy vital_wide_meta to '/tmp/vital_filter_trunc_mdctn_name_meta.csv' delimiter '!';
copy icd_filter_trunc_norm_wide_meta to '/tmp/icd_filter_trunc_mdctn_name_meta.csv' delimiter '!';
