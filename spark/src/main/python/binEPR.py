import os
import os.path
import sys
import json
import pandas as pd
from preprocUtils import *

input_file, crosswalk_file, icees_dir, pat_geo_file, output_file, *study_periods = sys.argv[1:]
df = pd.read_csv(input_file, quotechar='"')

cw = pd.read_csv(crosswalk_file)

df = df[(df["TLR4_AGE"].isna() | df["TLR4_AGE"] < 90) & (df["CURRENT_AGE"].isna() | df["CURRENT_AGE"] < 90) & (df["QXAGE"].isna() | df["QXAGE"] < 90)]

df.drop(["BMI", "HE_COMPLETION_DATE", "GEOID", "ROUTE_ID"], axis=1, inplace=True)

bins = []                                                                                                                                                
                                                                                                                                                         
bins += preprocAge(df, "TLR4_AGE")                                                                                                                       
                                                                                                                                                         
bins += preprocAge(df, "CURRENT_AGE")                                                                                                                    

bins += preprocAge(df, "QXAGE")                                                                                                                          

bins += preprocAge(df, "D28A_ASTHMA_AD_TEXT")                                                                                                            

bins += cut(df, "TLR4_DIST_1X", 5)                                                                                                                       

bins += cut(df, "TLR4_DIST_2X", 5)                                                                                                                       

bins += cut(df, "TLR4_DIST_3X", 5)                                                                                                                       

bins += cut(df, "O3_ANNUAL_AVERAGE", 5)                                                                                                                  

bins += cut(df, "PM25_ANNUAL_AVERAGE", 5)                                                                                                                

df["ESTTOTALPOP"], binsadd = pd.cut(df["ESTTOTALPOP"], [0,2500,50000,float("inf")], labels=["1","2","3"], include_lowest=True, right=False, retbins=True)

bins += [("ESTTOTALPOP", binsadd.tolist())]                                                                                                              

bins += cut(df, "ESTTOTALPOP25PLUS", 5)                                                                                                                  
bins += cut(df, "ESTPROPPERSONSNONHISPWHITE", 4)                                                                                                         
 # quantile(df, "EstProbabilityHouseholdNonHispWhite", 4)
bins += cut(df, "ESTPROPPERSONS25PLUSHSMAX", 4)                                                                                                          
bins += cut(df, "ESTPROPHOUSEHOLDSNOAUTO", 4)                                                                                                            
bins += cut(df, "ESTPROPPERSONSNOHEALTHINS", 4)                                                                                                          
bins += cut(df, "ESTPROPPERSONS5PLUSNOENGLISH", 4)                                                                                                       
bins += cut(df, "MEDIANHOUSEHOLDINCOME", 5)                                                                                                              
df.drop(["ESTTOTALPOP_SE", "ESTTOTALPOP25PLUS_SE", "ESTPROPPERSONSNONHISPWHITE_SE", "ESTPROPPERSONS25PLUSHSMAX_SE", "ESTPROPHOUSEHOLDSNOAUTO_SE", "ESTPROPPERSONSNOHEALTHINS_SE", "ESTPROPPERSONS5PLUSNOENGLISH_SE", "MEDIANHOUSEHOLDINCOME_SE"], axis=1, inplace=True)
df["DISTANCE2"] = pd.cut(df["DISTANCE"].apply(preprocHighwayExposure), [0, 50, 100, 150, 200, 250, float("inf")], labels=list(map(str, [1, 2, 3, 4, 5, 6])), include_lowest=True, right=False)
df["DISTANCE"] = pd.cut(df["DISTANCE"].apply(preprocHighwayExposure), [0, 50, 100, 200, 300, 500, float("inf")], labels=list(map(str, [1, 2, 3, 4, 5, 6])), include_lowest=True, right=False)
dir_path = os.path.dirname(output_file)
os.makedirs(dir_path, exist_ok=True)
df.to_csv(output_file, index=False)

df_pat = df.merge(cw, how="left")
df_pat["IN_EPR"] = 1
df_pat.to_csv(output_file+"_pat", index=False)


df_pat_geo = pd.read_csv(pat_geo_file)
df_hash_value = df[["HASH_VALUE"]]
df_pat_ord = df_hash_value.merge(cw, on="HASH_VALUE", how="left").merge(df_pat_geo[["patient_num"]], on="patient_num", how="outer")
df_pat_ord["index"] = df_pat_ord.index


with open(f"{output_file}_bins.json", "w") as out:
    json.dump(dict(bins), out)                    


def convert_float_to_int(df):
    return df.applymap(lambda x: int(x) if isinstance(x, float) and float.is_integer(x) else x)

    
for year in study_periods:
    print(year)
    dfp = pd.read_csv(f"{icees_dir}/{year}patient", index_col=0)
    dfp["IN_ICEES"] = 1
    dfpe = df_pat_ord.merge(df_pat.merge(dfp, on="patient_num", how="outer"), on=["patient_num", "HASH_VALUE"], how="right")
    dfpe["IN_ICEES"].fillna(0, inplace=True)
    dfpe["IN_EPR"].fillna(0, inplace=True)
    dfpe["study_period"].fillna(year, inplace=True)
    dfpe.sort_values(by=["index"], inplace=True)
    dfpe = convert_float_to_int(dfpe)
    dfpe.to_csv(f"{output_file}{year}patient", index=False)
    dfpe.drop(["patient_num", "HASH_VALUE"], axis=1).to_csv(f"{output_file}{year}patient_deidentified", index=False)

    dfp = pd.read_csv(f"{icees_dir}/{year}visit")
    dfp["IN_ICEES"] = 1
    dfpe = df_pat.merge(dfp, on="patient_num", how="outer")
    dfpe["IN_ICEES"].fillna(0, inplace=True)
    dfpe["IN_EPR"].fillna(0, inplace=True)
    dfpe["study_period"].fillna(year, inplace=True)
    dfpe["index"] = dfpe.index
    dfpe = convert_float_to_int(dfpe)
    dfpe.to_csv(f"{output_file}{year}visit", index=False)
    dfpe.drop(["patient_num", "HASH_VALUE"], axis=1).to_csv(f"{output_file}{year}visit_deidentified", index=False)

df.drop("HASH_VALUE", axis=1).to_csv(output_file+"_deidentified", index=False)
