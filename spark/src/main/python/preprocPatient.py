import pandas as pd
import numpy as np
import sys
import json
import os
import os.path
from preprocUtils import *

def preproc_patient(input_conf, input_file, output_file, pat_idx_df):

    _, patient_cols, _ = getBinary(input_conf)

    df = pd.read_csv(input_file, quotechar='"')

    patient_cols = set(patient_cols) & set(df.columns)

    bins = []                                                                                                                                                         
                                                                                                                                                                      
    bins += preprocAge(df, "AgeStudyStart")                                                                                                                           
                                                                                                                                                                      
    bins += preprocEnv(df, "Daily")                                                                                                                                  
                                                                                                                                                                      
    bins += preprocSocial(df)                                                                                                                                         
                                                                                                                                                                      
    addSex2(df)

    for c in patient_cols:
        df[c].fillna(0, inplace=True)
        df[c] = cut_col(df[c])

    df.drop(["birth_date"], axis=1, inplace=True)
    # add index column
    df = df.merge(pat_idx_df, how='left', on='patient_num')
    # sort by index column and move index column to the first column
    df = df.sort_values(by=['index'])
    index_col = df.pop('index')
    df.insert(0, 'index', index_col)
    df.to_csv(output_file, index=False)                                                                                                                              
                                                                                                                                                                      
    output_file_deidentified = output_file+"_deidentified"                                                                                                           
    df.drop(["patient_num"], axis=1).to_csv(output_file_deidentified, index=False)                                                                                  
    output_file_bins = output_file + "_bins.json"

    dir_path = os.path.dirname(output_file_bins)
    os.makedirs(dir_path, exist_ok=True)

    with open(output_file_bins, "w") as output_file_bins_stream:
        json.dump(dict(bins), output_file_bins_stream)
    
