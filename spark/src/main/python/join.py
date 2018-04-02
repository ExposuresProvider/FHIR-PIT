import pandas as pd
import sys
import glob

dir = sys.argv[1]

output_file = sys.argv[2]

def load(file):
    pd.read_csv(file,sep="!")

# https://stackoverflow.com/questions/29929639/when-combining-pandas-dataframe-concat-or-append-can-i-set-the-default-value
def combine(a, b):
    common_columns = a.columns.union(b.columns)

    df1 = a.reindex(columns=common_columns, fill_value='')
    df2 = b.reindex(columns=common_columns, fill_value='')

    return pd.concat([df1, df2], axis=0, ignore_index=True)

df = reduce(combine, map(load, glob.glob(dir, False)))
