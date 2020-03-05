import pandas as pd
import numpy as np
import sys
from preprocUtils import *

def preproc_patient(input_file, output_file):
    df = pd.read_csv(input_file)

    preprocAge(df, "AgeStudyStart")

    preprocEnv(df, "Daily")

    preprocSocial(df)

    addSex2(df)

    df.drop("birth_date", axis=1).to_csv(output_file, index=False)
    df.to_csv(output_file, index=False)

    output_file = output_file+"_deidentified"
    df.drop("patient_num", axis=1).to_csv(output_file, index=False)
