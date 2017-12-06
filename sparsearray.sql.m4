
define(`generate_array_sparse', `
drop type if exists $2 cascade; 

create type $2 as (indices integer[], elements $1[]); 

create or replace function $2_cast(a $1[]) returns $2 as $$
declare
  indices integer[];
  elements $1[];
begin
  indices := ARRAY[] :: integer[];
  elements := ARRAY[] :: $1[];
  for i in 1 .. array_upper(a,1) loop
    if a[i] is not null then
      indices := indices || i;
      elements := elements || a[i];
    end if;
  end loop;
  return (indices, elements);
end; $$ language plpgsql;

create cast ($1[] as $2) with function $2_cast($1[]) as assignment;

create or replace function $2_castback(a $2) returns $1[] as $$
declare
  elements $1[];
begin
  elements := ARRAY[] :: $1[];
  for i in 1 .. array_upper(a.indices,1) loop
    elements[a.indices[i]] := a.elements[i];
  end loop;
  return elements;
end; $$ language plpgsql;

create cast ($2 as $1[]) with function $2_castback($2) as assignment;

create or replace function sparse_array_name(a $1[]) returns text as $$
begin
  return ''`$2''`;
end; $$ language plpgsql;
')

generate_array_sparse(`text', `text_array_sparse')
generate_array_sparse(`numeric', `numeric_array_sparse')
generate_array_sparse(`integer', `integer_array_sparse')
generate_array_sparse(`varchar', `varchar_array_sparse')
generate_array_sparse(`timestamp', `timestamp_array_sparse')
generate_array_sparse(`boolean', `boolean_array_sparse')



