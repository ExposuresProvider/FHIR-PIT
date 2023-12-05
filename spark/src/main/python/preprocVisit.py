import pandas as pd
import json
import os
from preprocUtils import getBinary, preprocEnv, preprocSocial, addSex2, cut_col, env_features, env_features2


def preproc_visit(input_conf, input_file, output_file):

    _, _, visit_cols = getBinary(input_conf)

    df = pd.read_csv(input_file, quotechar='"')

    visit_cols = set(visit_cols) & set(df.columns)
    
    bins = []

    bins += preprocEnv(df, "24h")

    bins += preprocSocial(df)

    addSex2(df)

    cols_to_drop = ["encounter_num", "esri_id", "esri_idn", "GEOID", "stcnty", "start_date", "next_date",
                    "year_x", "year_y"] + \
                   [feature + stat for feature in env_features2 for stat in ["_avg", "_max", ""]] + \
                   [feature + stat + "_prev_date" for feature in env_features for stat in ["_avg", "_max"]]

    for c in visit_cols:
        df[c].fillna(0, inplace=True)
        df[c] = cut_col(df[c])

    for col in cols_to_drop:
        try:
            df = df.drop(col, axis=1)
        except Exception as e:
            print(e)

    df.to_csv(output_file, index=False)

    output_file_bins = output_file + "_bins.json"
    dir_path = os.path.dirname(output_file_bins)
    os.makedirs(dir_path, exist_ok=True)
    with open(output_file_bins, "w") as output_file_bins_stream:
        json.dump(dict(bins), output_file_bins_stream)

