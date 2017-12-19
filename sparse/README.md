# Loading sparse wide format table into Python #

## Copy data and metadata files ##

Currently you need

`endotype-wide.csv`

`mdctn_meta.csv`

`loinc_meta.csv`

`icd_meta.csv`

`endotype_meta.csv`

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

