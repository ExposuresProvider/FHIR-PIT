import glob
import sys
import os
import pandas as pd

input_file = sys.argv[1]
output_dir = sys.argv[2]

df = pd.read_csv(input_file)
df = df[["row", "col", "a","o3","pmij"]]
for group in df.groupby(["row", "col"]):
    row = group.index[0]
    col = group.index[1]
    output_rowcol_filename = os.path.join(output_dir, "R{0:03d}C{1:03d}.csv".format(row, col))
    group.to_csv(output_rowcol_filename, index=False)

