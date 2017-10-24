/* require postgis >= 2.3, postgres >= 9.5
   Ubuntu Xenial:
   https://launchpad.net/~ubuntugis/+archive/ubuntu/ppa?field.series_filter= */

/* import table cmaq_7da_DES generated from environmental.sql */

create extension if not exists postgis;

create extension if not exists postgis_topology;                                                                                               

/* http://spatialreference.org/ref/sr-org/29/ */
INSERT into spatial_ref_sys (srid, auth_name, auth_srid, proj4text, srtext) select 929, 'sr-org', 29, '+proj=lcc +lat_1=33 +lat_2=45 +lat_0=40 +lon_0=-97 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs ', 'PROJCS["WGC 84 / WRF Lambert",GEOGCS["WGS 84",DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137.0,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0.0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943295],AXIS["Geodetic longitude",EAST],AXIS["Geodetic latitude",NORTH],AUTHORITY["EPSG","4326"]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["central_meridian",-97.0],PARAMETER["latitude_of_origin",40.0],PARAMETER["standard_parallel_1",33.0],PARAMETER["false_easting",0.0],PARAMETER["false_northing",0.0],PARAMETER["standard_parallel_2",45.0],UNIT["m",1.0],AXIS["Easting",EAST],AXIS["Northing",NORTH]]' where not exists (select srid from spatial_ref_sys where srid = 929);

create or replace function filter_icd(concept_cd text) returns boolean as $$
  begin
    return concept_cd similar to '(ICD9:493|ICD9:464|ICD9:496|ICD9:786|ICD9:481|ICD9:482|ICD9:483|ICD9:484|ICD9:485|ICD9:486|ICD10:J45|ICD10:J05|ICD10:J44|ICD10:J66|ICD10:R05|ICD10:J12|ICD10:J13|ICD10:J14|ICD10:J15|ICD10:J16|ICD10:J17|ICD10:J18)%';
  end;
$$ language plpgsql;

/* create or replace function pm25_table(nds integer) returns void as $$
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
$$ language plpgsql; */

create type coors as (row integer, col integer);

create or replace function latlon2rowcol(latitude numeric, longitude numeric, year integer) returns coors as $$
declare
    row_no integer;
    col_no integer;
    number_of_columns integer;
    number_of_rows integer;
    xcell numeric;
    ycell numeric;
    xorig numeric;
    yorig numeric;
    p1 geometry;
    x1 numeric;
    y1 numeric;
begin
    -- CMAQ uses Lambert Conformal Conic projection
    row_no := -1;
    col_no := -1;

    if year = 2010 or year = 2011 then

        if year = 2010 then
            number_of_columns := 148;
            number_of_rows := 112;
            xcell := 36000.0;
            ycell := 36000.0;
        else
            number_of_columns := 459;
            number_of_rows := 299;
            xcell := 12000.0;
            ycell := 12000.0;
        end if;
        xorig := -2556000.0;
        yorig := -1728000.0;


        p1 := ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 929);

        x1 := ST_X(p1);
        y1 := ST_Y(p1);

        -- verify the points are in the grid
        if((x1>=xorig) and (x1<=(xorig+(xcell*number_of_columns))) and
           (y1>=yorig) and (y1<=(yorig+(ycell*number_of_rows)))) then
        
            -- find row and column in grid     
            col_no := floor(abs((xorig)-x1)/xcell) + 1;
            row_no := floor((abs(yorig)+y1)/ycell) + 1;
        end if;

    end if;
    return (row_no, col_no);
end $$ language plpgsql;

/* create or replace type pair_text as (fst text, snd text);

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
$$ language plpgsql; */


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

drop table if exists rowcol_asthma;
drop index if exists rowcol_asthma_index;
drop index if exists rowcol_asthma_index2;
create table rowcol_asthma as 
    select encounter_num, patient_num, (a.coors).row as row, (a.coors).col as col, date 
    from (
        select encounter_num, patient_num, latlon2rowcol(lat, long, date_part('year', start_date) :: integer) as coors, date_trunc('day', start_date) as date 
        from latlong_asthma inner join start_date_asthma using (patient_num)) as a
    where (a.coors).row <> -1 and (a.coors).col <> -1;
create index rowcol_asthma_index on rowcol_asthma (encounter_num, patient_num);      
create index rowcol_asthma_index2 on rowcol_asthma (col, row, date);      

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

/* do $$ begin
  for i in 1..7 loop
    perform pm25_table(i);
  end loop;
end $$; */

drop table if exists icd_asthma;
drop index if exists icd_asthma_index;
create table icd_asthma as select distinct encounter_num, patient_num, concept_cd as icd_code from asthma inner join rowcol_asthma using (encounter_num, patient_num) where filter_icd(concept_cd);
create index icd_asthma_index on icd_asthma ( encounter_num, patient_num);                                                                                                                                   

/* do $$ begin
  perform observation_table('loinc', array[('concept_cd', 'loinc_concept'), ('nval_num', 'loinc_nval'), ('units_cd', 'loinc_units'), ('start_date', 'loinc_start_date')] :: pair_text[], 'LOINC:%', true);
end $$; 

do $$ begin
  perform observation_table('mdctn', array[('concept_cd', 'mdctn_concept'), ('modifier_cd', 'mdctn_modifier'), ('nval_num', 'mdctn_nval'), ('tval_char', 'mdctn_tval'), ('units_cd', 'mdctn_units'), ('start_date', 'mdctn_start_date'), ('end_date', 'mdctn_end_date')] :: pair_text[], 'MDCTN:%', true);
end $$; */

drop table if exists features;
create table features as select *, extract(day from start_date - birth_date) as age from ed_visits_asthma inner join patient_reduced using (patient_num) inner join rowcol_asthma using (encounter_num, patient_num) inner join cmaq_7da_DES using (col, row, date);

copy features to '/tmp/endotype3.csv' delimiter ',' csv header;
/* copy loinc to '/tmp/loinc3.csv' delimiter ',' csv header;
copy mdctn to '/tmp/mdctn3.csv' delimiter ',' csv header; */
copy icd_asthma to '/tmp/icd3.csv' delimiter ',' csv header;



