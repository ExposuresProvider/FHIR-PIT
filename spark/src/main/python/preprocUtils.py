import pandas as pd
import numpy as np
import yaml

def getBinary(input_conf):  
    with open(input_conf) as icf:
        configuration = yaml.safe_load(icf)

    binary = list(configuration["FHIR"].keys())

    patient_cols = binary
    visit_cols = [f"{col}Visit" for col in binary]
    return (binary, patient_cols, visit_cols)

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


def cut_col(col):
    print(col.name, col.describe())
    try:
        new_col = pd.cut(col, [-float("inf"), 0.5, float("inf")], right=False, include_lowest=True, labels=["0", "1"])
    except TypeError as t:
        print(f"ERROR: {t}")
        print(f"===== Keeping col {col.name} the same =====")
        new_col = col
    return new_col


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
            return [(column, bins.tolist())]
        except Exception as e:
            print(f"cannot bin {col}: {e}")
            df[column] = None
            return [(column, None)]
    else:
        print(f"cannot find column {col}")
        return []


def preprocHighwayExposure(i):
    if i < 0:
        return 500
    else:
        return i

    
def preprocAge(df, col):
    df[f"{col}2"], bins_bigger = pd.cut(df[col], [np.NINF, 5, 18, 45, 65, 90], labels=['<5', '5-17', '18-44', '45-64', '65-89'], include_lowest=False, right=False, retbins=True)
    df[f"{col}"], bins_smaller = pd.cut(df[col], [np.NINF, 3, 18, 35, 51, 70, 90], labels=['0-2', '3-17', '18-34', '35-50', '51-69', '70-89'], include_lowest=False, right=False, retbins=True)
    return [
        (f"{col}2", bins_bigger.tolist()),
        (col, bins_smaller.tolist())
    ]


def cut(df, col, n):
    qcut_bins = quantile(df, col, n, "qcut", f"{col}_qcut")
    cut_bins = quantile(df, col, n, "cut", f"{col}_cut")
    df.drop([col], axis=1, inplace=True)
    return qcut_bins + cut_bins


def preprocEnv(df, period):
    bins = []
    for binning, binstr in [("_qcut", "qcut"), ("", "cut")]:
        for feature in features:
            for stat in ["Avg", "Max"]:
                for suffix in ["_StudyAvg", "_StudyMax", ""]:
                    col = stat + period + feature + "Exposure" + suffix
                    bins += quantile(df, col, 5, binstr, col + binning)
        for feature in features2:
            for stat in ["Avg", "Max"]:
                col_2 = stat + period + feature + "Exposure_2"
                bins += quantile(df, col_2, 5, binstr, col_2 + binning)
    return bins


def preprocSocial(df):
    bins = []
    df["EstResidentialDensity"], binsadd = pd.cut(df["EstResidentialDensity"], [0,2500,50000,float("inf")], labels=["1","2","3"], include_lowest=True, right=False, retbins=True)
    bins += [("EstResidentialDensity", binsadd.tolist())]
    bins += quantile(df, "EstResidentialDensity25Plus", 5)
    bins += quantile(df, "EstProbabilityNonHispWhite", 4)
    bins += quantile(df, "EstProbabilityHouseholdNonHispWhite", 4)
    bins += quantile(df, "EstProbabilityHighSchoolMaxEducation", 4)
    bins += quantile(df, "EstProbabilityNoAuto", 4)
    bins += quantile(df, "EstProbabilityNoHealthIns", 4)
    bins += quantile(df, "EstProbabilityESL", 4)
    bins += quantile(df, "EstHouseholdIncome", 5)

    df["MajorRoadwayHighwayExposure"].replace(float("inf"), 500, inplace=True)
    df["RoadwayDistanceExposure"].replace(float("inf"), 500, inplace=True)

    df["MajorRoadwayHighwayExposure2"], binsadd = pd.cut(df["MajorRoadwayHighwayExposure"].astype(float), [0, 50, 100, 150, 200, 250, float("inf")], labels=list(map(str, [1, 2, 3, 4, 5, 6])), include_lowest=True, right=False, retbins=True)
    bins += [("MajorRoadwayHighwayExposure2", binsadd.tolist())]
    df["MajorRoadwayHighwayExposure"], binsadd = pd.cut(df["MajorRoadwayHighwayExposure"].astype(float), [0, 50, 100, 200, 300, 500, float("inf")], labels=list(map(str, [1, 2, 3, 4, 5, 6])), include_lowest=True, right=False, retbins=True)
    bins += [("MajorRoadwayHighwayExposure", binsadd.tolist())]
    df["RoadwayDistanceExposure2"], binsadd = pd.cut(df["RoadwayDistanceExposure"].astype(float), [0, 50, 100, 150, 200, 250, float("inf")], labels=list(map(str, [1, 2, 3, 4, 5, 6])), include_lowest=True, right=False, retbins=True)
    bins += [("RoadwayDistanceExposure2", binsadd.tolist())]
    df["RoadwayDistanceExposure"], binsadd = pd.cut(df["RoadwayDistanceExposure"].astype(float), [0, 50, 100, 200, 300, 500, float("inf")], labels=list(map(str, [1, 2, 3, 4, 5, 6])), include_lowest=True, right=False, retbins=True)
    bins += [("RoadwayDistanceExposure", binsadd.tolist())]
    df["CAFO_Exposure"], binsadd = pd.cut(df["CAFO_Exposure"].astype(float), [0, 500, 1000, 2000, 4000, float("inf")], labels=list(map(str, [1, 2, 3, 4, 5])), include_lowest=True, right=False, retbins=True)
    bins += [("CAFO_Exposure", binsadd.tolist())]
    df["Landfill_Exposure"], binsadd = pd.cut(df["Landfill_Exposure"].astype(float), [0, 500, 1000, 2000, 4000, float("inf")], labels=list(map(str, [1, 2, 3, 4, 5])), include_lowest=True, right=False, retbins=True)
    bins += [("Landfill_Exposure", binsadd.tolist())]


    return bins
