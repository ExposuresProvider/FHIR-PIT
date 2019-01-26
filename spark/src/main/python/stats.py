import pandas as pd

import sys

filename = sys.argv[1]

df = pd.read_csv(filename)

columns = list(df.columns.values)

for column in columns:
    print(df[column].value_counts())
