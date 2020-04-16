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

    patient_cols = ["AsthmaDx", "CoughDx", "FibromyalgiaDx", "AnxietyDx", "Albuterol", "Ipratropium", "Sertraline", "DiabetesDx", "ObesityDx", "DepressionDx", "Diphenhydramine", "DrugDependenceDx", "Fluticasone", "Salmeterol", "Formoterol", "ReactiveAirwayDx", "Escitalopram", "Hydroxyzine", "Beclomethasone", "Fexofenadine", "PneumoniaDx", "OvarianDysfunctionDx", "PregnancyDx", "MenopauseDx", "TesticularDysfunctionDx", "EndometriosisDx", "AlcoholDependenceDx", "Fluoxetine", "CervicalCancerDx", "AutismDx", "Paroxetine", "Mometasone", "Budesonide", "Venlafaxine", "CroupDx", "Estradiol", "AlopeciaDx", "Arformoterol", "Mepolizumab", "Testosterone", "KidneyCancerDx", "UterineCancerDx", "OvarianCancerDx", "Medroxyprogresterone", "Propranolol", "TesticularCancerDx", "Theophylline", "Ciclesonide", "Flunisolide", "Tamoxifen", "Indacaterol", "Prasterone", "Progesterone", "Leuprolide", "Estropipate", "Goserelin", "Prednisone", "Cetirizine", "Citalopram", "Histrelin", "Triptorelin", "ProstateCancerDx", "Metaproterenol", "Omalizumab", "Trazodone", "Duloxetine", "Estrogen", "Androstenedione", "Nandrolone"]

    df.drop(["birth_date"], axis=1, inplace=True)

    for c in patient_cols:
        df[c].fillna(0, inplace=True)

    df.to_csv(output_file, index=False)

    output_file = output_file+"_deidentified"
    df.drop(["patient_num"], axis=1).to_csv(output_file, index=False)
