import pandas as pd

def quantile(df, col, n, bin="qcut", column=None):
    if not column:
        column = col
    if bin == "qcut":
        df[column] = pd.qcut(df[col], n, labels=list(map(str, range(1,n+1))))
    elif bin == "cut":
        df[column] = pd.cut(df[col], n, labels=list(map(str, range(1,n+1))))
    else:
        raise "unsupported binning method"

def preprocHighwayExposure(i):
    if i < 0:
        return 500
    else:
        return i

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
    df["MajorRoadwayHighwayExposure"] = pd.cut(df["MajorRoadwayHighwayExposure"].apply(preprocHighwayExposure), [0, 50, 100, 200, 300, 500, float("inf")], labels=list(map(str, [1, 2, 3, 4, 5, 6])), include_lowest=True, right=False)

