import pandas as pd
import sys
from preprocUtils import quantile, preprocSocial

input_file = sys.argv[1]
output_file = sys.argv[2]
year = sys.argv[3]
binstr = sys.argv[4]

df = pd.read_csv(input_file)

df["MepolizumabVisit"] = 0
quantile(df, "Avg24hPM2.5Exposure", 5, "qcut", "Avg24hPM2.5Exposure_qcut")
quantile(df, "Max24hPM2.5Exposure", 5, "qcut", "Max24hPM2.5Exposure_qcut")
quantile(df, "Avg24hOzoneExposure", 5, "qcut", "Avg24hOzoneExposure_qcut")
quantile(df, "Max24hOzoneExposure", 5, "qcut", "Max24hOzoneExposure_qcut")
quantile(df, "Avg24hPM2.5Exposure", 5, binstr)
quantile(df, "Max24hPM2.5Exposure", 5, binstr)
quantile(df, "Avg24hOzoneExposure", 5, binstr)
quantile(df, "Max24hOzoneExposure", 5, binstr)
preprocSocial(df)

df["year"] = year

df.to_csv(output_file, index=False)
