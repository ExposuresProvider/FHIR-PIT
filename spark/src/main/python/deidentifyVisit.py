import pandas as pd
import numpy as np
import sys
import json
import os
import os.path
from preprocUtils import *

def preproc_visit(input_conf, input_file, output_file):


    df = pd.read_csv(input_file, quotechar='"')

    cols_to_drop = ["birth_date"] 

    for col in cols_to_drop:
        try:
            df = df.drop(col, axis=1)
        except Exception as e:
            print(e)

    df.to_csv(output_file, index=False)

    output_file_deidentified = output_file+"_deidentified"

    df.drop(["patient_num"], axis=1).to_csv(output_file_deidentified, index=False)
