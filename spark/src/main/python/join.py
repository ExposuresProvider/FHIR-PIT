import pandas as pd
import sys
import glob

dir = sys.argv[1]

output_file = sys.argv[2]


dfs = []
count = 0
common_columns = pd.Index([])
for file in glob.glob(dir):
    count += 1
    print("loading " + str(count) + " " + file)
    df2 = pd.read_csv(file,sep="!")
    dfs.append((file, df2))
    common_columns = common_columns.union(df2.columns)

df1 = pd.DataFrame(columns = common_columns)
count = 0
# https://stackoverflow.com/questions/29929639/when-combining-pandas-dataframe-concat-or-append-can-i-set-the-default-value
for file, df2 in dfs:
    count += 1
    print("reindexing " + str(count) + " " + file)
    df2 = df2.reindex(columns=common_columns, fill_value='')
    df1 = pd.concat([df1, df2], axis=0, ignore_index=True)

df1.to_csv(output_file, sep="!", index=False)