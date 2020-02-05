import csv
import sys
import json
import openpyxl
from openpyxl import load_workbook

input_file_name, output_file_name = sys.argv[1:]

wb = load_workbook(input_file_name)
ws = wb["FeatureVariables"]

rd = {}

cols = ws.columns
print(type(cols))
next(cols)
for col in cols:
    pvar = col[5].value
    vvar = col[6].value
    if pvar is None:
        if vvar is None:
            break
    else:
        print(f"pvar = {pvar}, vvar = {vvar}")
        identifiers = []
        for identifier in col[8:]:
            cell = identifier.value
            if cell is not None:
                cell = cell.lower()
                if cell.startswith("rxcui"):
                    cell = cell[5:].replace(" = ","").replace(":", "")
                    identifiers.append(cell)
                    rd[cell] = pvar
            
with open(output_file_name, "w+") as of:
    json.dump(rd, of)


