import pandas as pd
import numpy as np
import sys
from preprocUtils import *

def preproc_visit(input_file, output_file):
    df = pd.read_csv(input_file)

    preprocAge(df, "AgeVisit")

    preprocEnv(df, "24h")

    preprocSocial(df)

    addSex2(df)

    visit_cols = ["AsthmaDxVisit", "CoughDxVisit", "FibromyalgiaDxVisit", "AnxietyDxVisit", "AlbuterolVisit", "IpratropiumVisit", "SertralineVisit", "DiabetesDxVisit", "ObesityDxVisit", "DepressionDxVisit", "DiphenhydramineVisit", "DrugDependenceDxVisit", "FluticasoneVisit", "SalmeterolVisit", "FormoterolVisit", "ReactiveAirwayDxVisit", "EscitalopramVisit", "HydroxyzineVisit", "BeclomethasoneVisit", "FexofenadineVisit", "PneumoniaDxVisit", "OvarianDysfunctionDxVisit", "PregnancyDxVisit", "MenopauseDxVisit", "TesticularDysfunctionDxVisit", "EndometriosisDxVisit", "AlcoholDependenceDxVisit", "FluoxetineVisit", "CervicalCancerDxVisit", "AutismDxVisit", "ParoxetineVisit", "MometasoneVisit", "BudesonideVisit", "VenlafaxineVisit", "CroupDxVisit", "EstradiolVisit", "AlopeciaDxVisit", "ArformoterolVisit", "MepolizumabVisit", "TestosteroneVisit", "KidneyCancerDxVisit", "UterineCancerDxVisit", "OvarianCancerDxVisit", "MedroxyprogresteroneVisit", "PropranololVisit", "TesticularCancerDxVisit", "TheophyllineVisit", "CiclesonideVisit", "FlunisolideVisit", "TamoxifenVisit", "IndacaterolVisit", "PrasteroneVisit", "ProgesteroneVisit", "LeuprolideVisit", "EstropipateVisit", "GoserelinVisit", "PrednisoneVisit", "CetirizineVisit", "CitalopramVisit", "HistrelinVisit", "TriptorelinVisit", "ProstateCancerDxVisit", "MetaproterenolVisit", "OmalizumabVisit", "TrazodoneVisit", "DuloxetineVisit", "EstrogenVisit", "AndrostenedioneVisit", "NandroloneVisit"]

    df = df.drop(["birth_date", "encounter_num", "esri_id", "esri_idn", "GEOID", "stcnty", "start_date", "next_date", "year_x", "year_y"] + [feature + stat for feature in env_features2 for stat in ["_avg", "_max", ""]] + [feature + stat + "_prev_date" for feature in env_features for stat in ["_avg", "_max"]], axis=1)

    for c in visit_cols:
        df[c].fillna(0, inplace=True)
        
    features = ["pm25", "o3"]
    
    features2 = ["ozone_daily_8hour_maximum", "pm25_daily_average", "CO_ppbv", "NO_ppbv", "NO2_ppbv", "NOX_ppbv", "SO2_ppbv", "ALD2_ppbv", "FORM_ppbv", "BENZ_ppbv"]
    
    df = df.drop(["birth_date", "encounter_num", "esri_id", "esri_idn", "GEOID", "stcnty", "next_date", "year_x", "year_y"] + [feature + stat for feature in features2 for stat in ["_avg", "_max", ""]] + [feature + stat + "_prev_date" for feature in features for stat in ["_avg", "_max"]], axis=1)
    
    df.to_csv(output_file, index=False)

    output_file = output_file+"_deidentified"

    df.drop(["patient_num"], axis=1).to_csv(output_file, index=False)
