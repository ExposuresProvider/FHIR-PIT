import sys
import csv

fn = sys.argv[1]

with open(fn) as f:
    content = f.readlines()

content = [x.strip().rstrip(",") for x in content]

codetoname = []
for row in content:
    entries = row.split("\t")
    medname = entries[0]
    medcodes = entries[1].split(",")
    for medcode in medcodes:
        codetoname.append((medcode, medname));

with open(sys.argv[2], 'w') as out:
    csv_out = csv.writer(out)
    for row in codetoname:
        csv_out.writerow(row)
    
