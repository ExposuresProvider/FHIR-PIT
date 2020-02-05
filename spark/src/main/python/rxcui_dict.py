import csv
import sys
import json
import openpyxl
from openpyxl import read_workbook

input_file_name, output_file_name = sys.argv[1:]

wb = read_workbook(input_file_name)
ws = wb["FeatureVariables"]

rd = []

for col in ws.iter_cols(min_col=1, min_row=6, values_only=True):
    pvar = next(col)
    vvar = next(col)
    next(col)
    identifiers = []
    while True:
        identifier = next(col)
        if identifer == "":
            break
        else:
            identifiers.append(identifier)
    rd.append({
        "patient_var": pvar,
        "visit_var": vvar,
        "identifiers": identifiers
    })
            
with open(output_file_name, "w+") as of:
    json.dump(rd, of)


