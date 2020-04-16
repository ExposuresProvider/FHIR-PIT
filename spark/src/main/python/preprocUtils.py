import pandas as pd
import numpy as np

features = ["PM2.5", "Ozone"]
features2 = ["PM2.5", "Ozone", "CO", "NO", "NO2", "NOx", "SO2", "Acetaldehyde", "Formaldehyde", "Benzene"]

env_features = ["pm25", "o3"]
env_features2 = ["ozone_daily_8hour_maximum", "pm25_daily_average", "CO_ppbv", "NO_ppbv", "NO2_ppbv", "NOX_ppbv", "SO2_ppbv", "ALD2_ppbv", "FORM_ppbv", "BENZ_ppbv"]

def sex2gen(a):
    if a == "Female" or a == "Male":
        return a
    else:
        return ""

def addSex2(df):
    df["Sex2"] = df["Sex"].apply(sex2gen)


def quantile(df, col, n, bin="qcut", column=None):
    # print(f"{bin} {col} -> {column}")                                                                                                                      
    if col in df:
        try:
            if column is None:
                column = col
            if bin == "qcut":
                df[column], bins = pd.qcut(df[col], n, labels=list(map(str, range(1,n+1))), retbins=True)
            elif bin == "cut":
                df[column], bins = pd.cut(df[col], n, labels=list(map(str, range(1,n+1))), retbins=True)
            else:
                raise "unsupported binning method"
        except Exception as e:
            print(f"cannot bin {col}: {e}")            
            df[column] = None
    else:
        print(f"cannot find column {col}")


        
def preprocHighwayExposure(i):
    if i < 0:
        return 500
    else:
        return i

def preprocAge(df, col):
    df[f"{col}2"] = pd.cut(df[col], [np.NINF, 5, 18, 45, 65, 90], labels=['<5', '5-17', '18-44', '45-64', '65-89'], include_lowest=False, right=False)
    df[f"{col}"] = pd.cut(df[col], [np.NINF, 3, 18, 35, 51, 70, 90], labels=['0-2', '3-17', '18-34', '35-50', '51-69', '70-89'], include_lowest=False, right=False)

def cut(df, col, n):
    quantile(df, col, n, "qcut", f"{col}_qcut")
    quantile(df, col, n, "cut", f"{col}_cut")
    df.drop([col], axis=1, inplace=True)


def preprocEnv(df, period):
    for binning, binstr in [("_qcut", "qcut"), ("", "cut")]:
        for feature in features:
            for stat in ["Avg", "Max"]:
                for suffix in ["_StudyAvg", "_StudyMax", ""]:
                    col = stat + period + feature + "Exposure" + suffix
                    quantile(df, col, 5, binstr, col + binning)
        for feature in features2:
            for stat in ["Avg", "Max"]:
                for suffix in ["_StudyAvg", "_StudyMax", ""]:
                    col_2 = stat + period + feature + "Exposure" + suffix + "_2"
                    quantile(df, col_2, 5, binstr, col_2 + binning)


def preprocSocial(df):
    df["EstResidentialDensity"] = pd.cut(df["EstResidentialDensity"], [0,2500,50000,float("inf")], labels=["1","2","3"], include_lowest=True, right=False)
    quantile(df, "EstResidentialDensity25Plus", 5)
    quantile(df, "EstProbabilityNonHispWhite", 4)
    quantile(df, "EstProbabilityHouseholdNonHispWhite", 4)
    quantile(df, "EstProbabilityHighSchoolMaxEducation", 4)
    quantile(df, "EstProbabilityNoAuto", 4)
    quantile(df, "EstProbabilityNoHealthIns", 4)
    quantile(df, "EstProbabilityESL", 4)
    quantile(df, "EstHouseholdIncome", 5)
    df["MajorRoadwayHighwayExposure2"] = pd.cut(df["MajorRoadwayHighwayExposure"].apply(preprocHighwayExposure), [0, 50, 100, 150, 200, 250, float("inf")], labels=list(map(str, [1, 2, 3, 4, 5, 6])), include_lowest=True, right=False)
    df["MajorRoadwayHighwayExposure"] = pd.cut(df["MajorRoadwayHighwayExposure"].apply(preprocHighwayExposure), [0, 50, 100, 200, 300, 500, float("inf")], labels=list(map(str, [1, 2, 3, 4, 5, 6])), include_lowest=True, right=False)
    df["RoadwayDistanceExposure2"] = pd.cut(df["RoadwayDistanceExposure"].apply(preprocHighwayExposure), [0, 50, 100, 150, 200, 250, float("inf")], labels=list(map(str, [1, 2, 3, 4, 5, 6])), include_lowest=True, right=False)
    df["RoadwayDistanceExposure"] = pd.cut(df["RoadwayDistanceExposure"].apply(preprocHighwayExposure), [0, 50, 100, 200, 300, 500, float("inf")], labels=list(map(str, [1, 2, 3, 4, 5, 6])), include_lowest=True, right=False)


