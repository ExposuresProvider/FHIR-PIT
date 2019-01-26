import pandas as pd

import sys

filename = sys.argv[1]

df = pd.read_csv(filename)

columns = list(df.columns.values)

for column in columns:
    vc = df[column].value_counts().sort_index()
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
      print(df)
