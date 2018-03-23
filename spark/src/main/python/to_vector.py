import datetime
import glob
import sys
import json
import vec_to_array

dir = sys.argv[1]
outputfile = sys.argv[2]

header0 = vec_to_array.header(dir + "/json/header")
mdctn_rxnorm_map = vec_to_array.mdctn_to_rxnorm_map(dir + "/mdctn_rxnorm_map.csv")

header0.extend(list(mdctn_rxnorm_map.keys()))

header_map = vec_to_array.header_map(header0)

def header_map2(feature):
    if feature in mdctn_rxnorm_map:
        feature2 = mdctn_rxnorm_map[feature]
        col = feature2["name"]
        rxcui = feature2["rxcui"]
        if rxcui == "Unclassed":
            val = feature
        else:
            val = rxcui
        return col, val
    elif feature.startswith("ICD"):
        return feature.split(".")[0], 1
    else:
        return feature, 1

start_date = datetime.datetime.strptime("2010-1-1", "%Y-%m-%D")
end_date = datetime.datetime.strptime("2012-1-1", "%Y-%m-%D")

def filter_json(mat):
    mat_date = datetime.datetime.strptime(mat["start_date"], "%Y-%m-%D")
    return mat_date >= start_date and mat_date < end_date

with open(outputfile, "w") as f:
    for filename in glob.glob(dir + "/json/vector*"):
        print("processing", filename)
        jsonlist = vec_to_array.vec_to_json(header_map, header_map2, filter_json, filename)
        for obj in jsonlist:
            json.dump(obj,f)
            f.write("\n")








