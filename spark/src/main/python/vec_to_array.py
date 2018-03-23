import json
import numpy as np
import re
import pandas as pd

prog = re.compile("""ICD9:((493|464|496|786|481|482|483|484|485|486)[.].*|278.00)|ICD10:((J45|J05|J44|J66|R05|J12|J13|J14|J15|J16|J17|J18)[.].*|E66[.]([^3].*|3.+))|"""
                  """LOINC:(33536-4|13834-7|26449-9|711-2|712-0|26450-7|713-8|714-6|26499-4|751-8|753-4|26511-6|770-8|23761-0|1988-5|30522-7|11039-5|35648-5|76485-2|76486-0|14634-0|71426-1)""")

def header(filename):

    with open(filename) as header_data:
        header0 = sorted(map(lambda x : x.rstrip(), header_data.readlines()))

    header = []
    for col_name in header0:
        if prog.match(col_name):
            header.append(col_name)
    return header

def header_map(header0):
    return dict(map(lambda x : (x[1], x[0]), enumerate(header0)))

def mdctn_to_rxnorm_map(filename):
    df = pd.read_csv(filename, sep="\t", names=["name","rxcui","concept_cd","description"], index_col=2)
    df = df[["name", "rxcui"]]
    return df.to_dict("index")

def vec_to_array(header_map, header_map2, filename):
    with open(filename) as json_data:
        d = json.load(json_data)

    time_series = d["data"]
    n_cols = len(header_map)
    n_rows = len(time_series)

    list1 = []
    list2 = []

    for row in time_series:
        mat = np.zeros((n_cols,))
        has_feature = False
        for feature in row["features"]:
            col, val = header_map2(feature)
            if col in header_map:
                col_index = header_map[col]
                mat[col_index] = val
                has_feature = True
        if has_feature:
            list1.append(mat)
            list2.append(row["start_date"])

    return np.array(list2), np.array(list1), d["sex_cd"], d["race_cd"], d["birth_date"]

def vec_to_json(col_index_map, header_map, filter_json, filename):
    with open(filename) as json_data:
        d = json.load(json_data)

    time_series = d["data"]
    n_cols = len(col_index_map)
    n_rows = len(time_series)

    list1 = []

    sex_cd = d["sex_cd"]
    race_cd = d["race_cd"]
    birth_date = d["birth_date"]
    for row in time_series:
        has_feature = False
        mat = {}
        for feature in row["features"]:
            col, val = header_map(feature)
            if col in col_index_map:
                col_index = col_index_map[col]
                mat[col_index] = val
                has_feature = True
        if has_feature:
            mat["age"] = row["age"]
            mat["start_date"] = row["start_date"]
            mat["sex_cd"] = sex_cd
            mat["race_cd"] = race_cd
            mat["birth_date"] = birth_date
            if filter_json(mat):
                list1.append(mat)

    return list1
