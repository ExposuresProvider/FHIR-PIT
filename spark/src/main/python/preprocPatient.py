import pandas as pd
import numpy as np
import sys
import json
from preprocUtils import *

def preproc_patient(input_file, output_file):
    df = pd.read_csv(input_file)

    bins = []                                                                                                                                                         
                                                                                                                                                                      
    bins += preprocAge(df, "AgeStudyStart")                                                                                                                           
                                                                                                                                                                      
    bins += preprocEnv(df, "Daily")                                                                                                                                  
                                                                                                                                                                      
    bins += preprocSocial(df)                                                                                                                                         
                                                                                                                                                                      
    addSex2(df)                                                                                                                                                       
                                                                                                                                                                      
    for c in patient_cols:                                                                                                                                            
         df[c].fillna(0, inplace=True)                                                                                                                               
        df[c] = cut_col(df[c])                                                                                                                                        
                                                                                                                                                                      
    df.drop(["birth_date"], axis=1, inplace=True)                                                                                                                    
                                                                                                                                                                      
    df.to_csv(output_file, index=False)                                                                                                                              
                                                                                                                                                                      
    output_file_deidentified = output_file+"_deidentified"                                                                                                           
    df.drop(["patient_num"], axis=1).to_csv(output_file_deidentified, index=False)                                                                                  
    output_file_bins = output_file + "_bins.json"

    with open(output_file_bins, "w") as output_file_bins_stream:
        json.dump(dict(bins), output_file_bins_stream)
    
