import csv
import sys
import json

input_file_name, output_file_name = sys.argv[1:]

rd = {}
with open(input_file_name) as f:
    r = csv.reader(f)
    for row in r:
        row = [x.strip().lower() for x in row]
        ids = [x for x in row if "rxcui" in x]
        for id in ids:
            rd[id[6:]] = row[0]
with open(output_file_name, "w+") as of:
    json.dump(rd, of)


