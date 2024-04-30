import pandas as pd


def deidentify_patient(input_file, output_file, pat_idx_file):
    df = pd.read_csv(input_file, quotechar='"')
    df.drop(["birth_date"], axis=1, inplace=True)
    # add index column
    pat_idx_df = pd.read_csv(pat_idx_file)
    df = df.merge(pat_idx_df, how='left', on='patient_num')
    # sort by index column and move index column to the first column
    df = df.sort_values(by=['index'])
    index_col = df.pop('index')
    df.insert(0, 'index', index_col)
    df.rename(columns={'study_period': 'year', 'Active_In_Study_Period': 'Active_In_Year'}, inplace=True)
    df.to_csv(output_file, index=False)                                                                                                                              
    output_file_deidentified = output_file+"_deidentified"                                                                                                           
    df.drop(["patient_num"], axis=1).to_csv(output_file_deidentified, index=False)                                                                                  
