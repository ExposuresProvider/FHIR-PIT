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
For example, print rows filtered by age <= 10:

```
    def cb(r):
        birth_date = datetime.strptime(r['birth_date'], "%Y-%m-%d %H:%M:%S")
        curr_date = datetime.now()
        age = curr_date.year - birth_date.year - ((curr_date.month, curr_date.day) < (birth_date.month, birth_date.day))
        if age <= 10:
            print(r)
```

# Loading into C++

If you want to preprocess the file, this is preferred approach. Modify `import.cpp` to do preprocessing.

```
g++ -o import -std=c++17 import.cpp import_df.cpp
```

If `c++17` is not available you can use `c++11` or `c++14`.