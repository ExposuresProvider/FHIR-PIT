import pandas as pd
import numpy as np
import sys
import json
import os
import os.path
from preprocUtils import *

def preproc_visit(input_conf, input_file, output_file):

    _, _, visit_cols = getBinary(input_conf)

    df = pd.read_csv(input_file, quotechar='"')

    visit_cols = set(visit_cols) & set(df.columns)
    
    # filter out non-number columns
    cols_type = df.dtypes
    visit_cols = [col for col in visit_cols if cols_type[col] == np.float or cols_type[col] == np.int]

    bins = []

    bins += preprocAge(df, "AgeVisit")

    bins += preprocEnv(df, "24h")

    bins += preprocSocial(df)

    addSex2(df)

    cols_to_drop = ["birth_date", "encounter_num", "esri_id", "esri_idn", "GEOID", "stcnty", "start_date", "next_date", "year_x", "year_y"] + [feature + stat for feature in env_features2 for stat in ["_avg", "_max", ""]] + [feature + stat + "_prev_date" for feature in env_features for stat in ["_avg", "_max"]]

    for c in visit_cols:
        df[c].fillna(0, inplace=True)
        df[c] = cut_col(df[c])

    for col in cols_to_drop:
        try:
            df = df.drop(col, axis=1)
        except Exception as e:
            print(e)

    features = ["pm25", "o3"]

    features2 = ["ozone_daily_8hour_maximum", "pm25_daily_average", "CO_ppbv", "NO_ppbv", "NO2_ppbv", "NOX_ppbv", "SO2_ppbv", "ALD2_ppbv", "FORM_ppbv", "BENZ_ppbv"]

    df.to_csv(output_file, index=False)

    output_file_deidentified = output_file+"_deidentified"

    df.drop(["patient_num"], axis=1).to_csv(output_file_deidentified, index=False)

    output_file_bins = output_file + "_bins.json"
    dir_path = os.path.dirname(output_file_bins)
    os.makedirs(dir_path, exist_ok=True)
    with open(output_file_bins, "w") as output_file_bins_stream:
        json.dump(dict(bins), output_file_bins_stream)

