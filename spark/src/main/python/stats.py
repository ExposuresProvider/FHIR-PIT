import pandas as pd

import sys
from tabulate import tabulate

filename = sys.argv[1]
filename2 = sys.argv[2]

df = pd.read_csv(filename)
df2 = pd.read_csv(filename2)

columns = list(df.columns.values)
columns2 = list(df2.columns.values)

columnSet = set(columns)
columnSet2 = set(columns2)

print("columns in first file not in second file:", columnSet - columnSet2)
print("columns not in first file in second file:", columnSet2 - columnSet)

def merge(index):
    n = len(index)
    i = [0] * n
    merged = []

    while any((i[j] < len(index[j]) for j in range(n))):
        nextVal = [index[j][i[j]] if i[j] < len(index[j]) else None for j in range(n)]
        val, inx = min(((val, inx) for (inx, val) in enumerate(nextVal) if val is not None))
        moveForward = (inx for (inx, val2) in enumerate(nextVal) if val2 == val)
        merged.append(val)
        for j in moveForward:
            i[j] += 1

    return merged
    
for column in [column for column in columns if column in columns2]:
    print("column:", column)
    dfc = df[column]
    dfc2 = df2[column]
    vc = dfc.value_counts().sort_index()
    vc2 = dfc2.value_counts().sort_index()

    if vc.count() > 10 and vc2.count() > 10:
        vc = dfc.describe()
        vc2 = dfc2.describe()
        

    indices = list(vc.index.values)
    indices2 = list(vc2.index.values)

    print(tabulate([[index, vc.get(index, default=0), vc2.get(index, default=0), vc.get(index, default=0) - vc2.get(index, default=0)] for index in merge([indices, indices2])]))
            

