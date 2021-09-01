import pandas as pd
import sys
import os

def load_file(filename):
    path = f"formatted/{filename}"
    if os.path.isfile(path):
        return path
    else:
        return f"src/{filename}"


for year in range(2010, 2015):
    dffn1 = load_file(f"cmaq_downscaling_12US1_459x299_{year}.txt")
    dffn2 = load_file(f"merged_cmaq_{year}.csv") 
    dffn3 = f"merged_cmaq_{year}.csv"
    print(f"reading {dffn1}")
    df1 = pd.read_csv(dffn1)
    print(f"reading {dffn2}")

    df2 = pd.read_csv(dffn2)

    print(f"writing {dffn3}")
    df = pd.merge(df1, df2, on=["FIPS", "Date"], how="inner", suffixes=("", "_2"))

    df.to_csv(dffn3, index=False)
