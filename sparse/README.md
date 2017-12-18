# install antlr 4 #

https://github.com/antlr/antlr4/blob/master/doc/python-target.md

# generate parser #

    antlr4 -Dlanguage=Python2 sparsecsv.g4

This step should generated the following python source file:

`sparsecsvParser.py`

`sparsecsvLexer.py`

`sparsecsvListener.py`

Make sure that they are in the same directory as `import.py`

# Copy data and metadata files #

Currently you need

`endotype-wide.csv`

`mdctn_meta.csv`

`loinc_meta.csv`

`icd_meta.csv`

`endotype_meta.csv`

# load csv #

```
from import_df import load_df
df = load_df(<filename>)
```
