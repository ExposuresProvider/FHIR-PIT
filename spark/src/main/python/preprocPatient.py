import pandas as pd
import json
import os
import os.path
from preprocUtils import getBinary, preprocEnv, preprocSocial, addSex2, cut_col


def preproc_patient(input_conf, input_file, output_file):

    _, patient_cols, _ = getBinary(input_conf)

    df = pd.read_csv(input_file, quotechar='"')

    patient_cols = set(patient_cols) & set(df.columns)

    bins = []                                                                                                                                                         
                                                                                                                                                                      
    bins += preprocEnv(df, "Daily")
                                                                                                                                                                      
    bins += preprocSocial(df)                                                                                                                                         
                                                                                                                                                                      
    addSex2(df)

    for c in patient_cols:
        df[c].fillna(0, inplace=True)
        df[c] = cut_col(df[c])

    df.to_csv(output_file, index=False)
                                                                                                                                                                      
    output_file_bins = output_file + "_bins.json"

    dir_path = os.path.dirname(output_file_bins)
    os.makedirs(dir_path, exist_ok=True)

    with open(output_file_bins, "w") as output_file_bins_stream:
        json.dump(dict(bins), output_file_bins_stream)
