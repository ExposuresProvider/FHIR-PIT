import pandas as pd
import sys
import glob

dir = sys.argv[1]

output_file = sys.argv[2]


df1 = None
for file in glob.glob(dir, False):
    df2 = pd.read_csv(file,sep="!")
    if df1 is None:
        df1 = df2
    else:
        # https://stackoverflow.com/questions/29929639/when-combining-pandas-dataframe-concat-or-append-can-i-set-the-default-value
        common_columns = df1.columns.union(df2.columns)
        df1 = df1.reindex(columns=common_columns, fill_value='')
        df2 = df2.reindex(columns=common_columns, fill_value='')

        df1 = pd.concat([df1, df2], axis=0, ignore_index=True)


df1.to_csv(output_file, sep="!", index=False)