import glob
import sys
import os
import pandas as pd

input_dir = sys.argv[1]
output_dir = sys.argv[2]
start_date = sys.argv[3]
end_date = sys.argv[4]

if len(sys.argv) >= 6:
    dirs = [os.path.join(input_dir, sys.argv[5])]
else:
    dirs = sorted(glob.glob(os.path.join(input_dir, "C*")))

for i, dirname in enumerate(dirs):
    print("processing", i, "/", len(dirs), dirname)
    output_rowcol_filename = os.path.join(output_dir, os.path.basename(dirname) + ".csv")
    filenames = sorted(glob.glob(os.path.join(dirname, "*.csv")))
    def load_csv(filename):
        df = pd.read_csv(filename)
        df = df[["Date","O3_ppb", "PM25_Total_ugm3"]]

        df2 = df[(start_date <= pd.to_datetime(df["Date"])) & (pd.to_datetime(df["Date"]) < end_date)]

        df2.columns = ["start_date", "o3", "pm25"]
        return df2
    df = pd.concat(list(map (load_csv, filenames)))

    df.to_csv(output_rowcol_filename, index=False)

