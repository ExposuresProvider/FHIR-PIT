import pandas as pd
import sys
from preprocUtils import quantile, preprocSocial

input_file = sys.argv[1]
output_file = sys.argv[2]
year = sys.argv[3]
binstr = sys.argv[4]

df = pd.read_csv(input_file)

df["Mepolizumab"] = 0
for binning, binstr in [("_qcut", "qcut"), ("", binstr)]:
    for feature in ["PM2.5", "Ozone"]:
        for stat in ["Avg", "Max"]:
            for suffix in ["_StudyAvg", "_StudyMax", ""]:
                quantile(df, stat + "Daily" + feature + "Exposure" + suffix, 5, binstr, stat + "Daily" + feature + "Exposure" + suffix + binning)
preprocSocial(df)

df["year"] = year

df.to_csv(output_file, index=False)
