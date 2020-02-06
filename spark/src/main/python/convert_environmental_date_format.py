import csv
import sys
import datetime

input_path, output_path = sys.argv[1:]

with open(input_path, newline="") as f:
    with open(output_path, "w", newline="") as f2:
        r = csv.reader(f)
        w = csv.writer(f2)
        header = next(r)
        w.writerow(header)
        for row in r:
            row[0] = datetime.datetime.strptime(row[0], "%b-%d-%Y").strftime("%Y/%m/%d")
            w.writerow(row)
