# Loading sparse wide format table into Python #

## Copy data and metadata files ##

Currently you need

`endotype-wide.csv`

`mdctn_rxnorm_meta.csv`

`mdctn_gene_meta.csv`

`loinc_meta.csv`

`icd_meta.csv`

`vital_meta.csv`

`endotype_meta.csv`

## Schema of generated wide-format table ##

### Simple columns ###

Wide-format table contains simple columns. In general, simple columns have the following format:

```<col>```

For available `col`, see `endotype_meta.csv`.

### Composite columns ###

And composite columns generated from the pivoting operation. In general, composite columns have the following format:

For `icd` table:

```<table>_<col>_<concept_cd>```

For `loinc` and `vital` tables:

```<table>_<col>_<concept_cd>_<instance_num>```

For `mdctn` table

```<table>_<col>_<concept_cd>_<modifier_cd>_<instance_num>```

For available `table` and `col`, see `endotype_meta.csv`.

For available `concept_cd`, `modifier_cd` and `instance_num`, see `<table>_meta.csv`.

The `table` is `icd`, `loinc`, `mdctn`, or `vital`.

The `col` is `code`, `valtype`, `valueflag`, `nval`, `tval`, `units`, `start_date`, or `end_date`. 

For example, the `loinc_valtype` of `concept_cd` `LOINC:711-2`, `instance_num` `1` in the long-format table is `loinc_valtype_LOINC:711-2_1` in the wide-format table.

## Load csv ##

The `load_df` function can be use to load a sparse csv file.

### Load it into a `pandas` `DataFrame` ###

```
from import_df import load_df
df = load_df(<filename>, filemeta = <filemeta>, colmeta = <colmeta>)
# the df.df will be the dataframe
```

### Load it row by row ###
```
from import_df import load_df
load_df(<filename>, <callback>, <filemeta>, <colmeta>)
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

The `<filemeta>` is a string of path to the metadata file. For example, `"endotype_meta.csv"`.
The `<colmeta>` is a list of pairs of table name and path to the metadata file for that table. For example, `[("icd", "icd_meta.csv"), ("mdctn", "mdctn_meta.csv"), ("loinc", "loinc_meta.csv"),("vital","vital_meta.csv")]`.

## Loading into C++ ##

If you want to preprocess the file, this is preferred approach. Modify `import.cpp` to do preprocessing.

```
g++ -o import -std=c++17 import.cpp import_df.cpp
```

If `c++17` is not available you can use `c++0x`, `c++11`, or `c++14`.
