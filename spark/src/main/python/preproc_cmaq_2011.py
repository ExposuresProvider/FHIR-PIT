import glob
import sys
import os
import pandas as pd

input_dir = sys.argv[1]
output_dir = sys.argv[2]

dirs = sorted(glob.glob(os.path.join(input_dir, "C*")))
for i, dirname in enumerate(dirs):
    print("processing", i, "/", len(dirs), dirname)
    output_rowcol_filename = os.path.join(output_dir, os.path.basename(dirname) + ".csv")
    filenames = sorted(glob.glob(os.path.join(dirname, "*.csv")))
    df = pd.concat(list(map (lambda filename : pd.read_csv(filename), filenames)))
    df.to_csv(output_rowcol_filename)

