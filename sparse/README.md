# Loading sparse wide format table into Python #

## Copy data and metadata files ##

Currently you need

`endotype-wide.csv`

`mdctn_meta.csv`

`loinc_meta.csv`

`icd_meta.csv`

`endotype_meta.csv`

## Schema of generated wide-format table ##

Wide-format table contains composite columns generated from the pivoting operation. In general, composite columns have the following format:

```<table>_<col>_<concept_cd>_<instance_num>```

The only exception is the 

```icd_code_<concept_cd>_<instance_num>```

This is a boolean value `t` or `f`.

For available `table` and `col`, see `endotype_meta.csv`.

For available `concept_cd` and `instance_num`, see `<table>_meta>.csv`.

The `table` is `icd_code`, `loinc`, `mdctn`, or `vital`.

The `col` is `modifier`, `valtype`, `valueflag`, `nval`, `tval`, `units`, `start_date`, or `end_date`. 

For example, the `loinc_valtype` of `concept_cd` `LOINC:711-2`, `instance_num` `1` in the long-format table is `loinc_valtype_LOINC:711-2_1` in the wide-format table.

## load csv ##

The `load_df` function can be use to load a sparse csv file.

### Load it into a `pandas` `DataFrame` ###

```
from import_df import load_df
df = load_df(<filename>)
# the df.df will be the dataframe
```

### Load it row by row ###
```
from import_df import load_df
load_df(<filename>, <callback>)
```

The `<callback>` function has the following format:

```
def cb(r):
    ...
```

where `r` is an object of the form:

```
{
   col_name_1 : col_value_1,
   ...
   col_name_n : col_value_n
}
```
For example, print rows filtered by age <= 10:

```
    def cb(r):
        birth_date = datetime.strptime(r['birth_date'], "%Y-%m-%d %H:%M:%S")
        curr_date = datetime.now()
        age = curr_date.year - birth_date.year - ((curr_date.month, curr_date.day) < (birth_date.month, birth_date.day))
        if age <= 10:
            print(r)
```



## Loading into C++ ##

If you want to preprocess the file, this is preferred approach. Modify `import.cpp` to do preprocessing.

```
g++ -o import -std=c++17 import.cpp import_df.cpp
```

If `c++17` is not available you can use `c++0x`, `c++11`, or `c++14`.
