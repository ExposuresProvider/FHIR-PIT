import csv
import sys
import json

input_file = sys.argv[1]
output_file = sys.argv[2]

obj = {}

with open(input_file, encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        var = row[0]
        rxnorms = list(filter(lambda x : "rxcui" in x, map(lambda x : x.lower(), row[2:])))
        for rxnorm in rxnorms:
            obj[rxnorm] = var
            
with open(output_file, "w+") as f2:
    json.dump(obj, f2)
