-- total
select count(*) from col_row_date_any_18_out;
select count(*) from col_row_date_asthma_18_ed;

-- asthma-like
select count (*) from (select distinct encounter_num, patient_num from col_row_date_any_18_out inner join observation_fact using (encounter_num, patient_num) where filter_icd(concept_cd)) as a;
select count (*) from (select distinct encounter_num, patient_num from col_row_date_asthma_18_ed inner join observation_fact using (encounter_num, patient_num) where filter_icd(concept_cd)) as a;

-- outpatient
select count(*) from (select distinct encounter_num, patient_num from col_row_date_any_18_out inner join visit_dimension using (encounter_num, patient_num) where inout_cd = 'OUTPATIENT') as a;
select count(*) from (select distinct encounter_num, patient_num from col_row_date_asthma_18_ed inner join visit_dimension using (encounter_num, patient_num) where inout_cd = 'OUTPATIENT') as a;

-- outpatient
select count(*) from (select distinct encounter_num, patient_num from col_row_date_any_18_out inner join visit_dimension using (encounter_num, patient_num) where inout_cd = 'INPATIENT') as a;
select count(*) from (select distinct encounter_num, patient_num from col_row_date_asthma_18_ed inner join visit_dimension using (encounter_num, patient_num) where inout_cd = 'INPATIENT') as a;

-- outpatient
select count(*) from (select distinct encounter_num, patient_num from col_row_date_any_18_out inner join visit_dimension using (encounter_num, patient_num) where inout_cd = 'EMERGENCY') as a;
select count(*) from (select distinct encounter_num, patient_num from col_row_date_asthma_18_ed inner join visit_dimension using (encounter_num, patient_num) where inout_cd = 'EMERGENCY') as a;

