import pandas as pd

import sys
from tabulate import tabulate

filenames = sys.argv[1:]

dfs = [pd.read_csv(filename) for filename in filenames]

columnss = [list(df.columns.values) for df in dfs]

columnSets = [set(columns) for columns in columnss]

n = len(filenames)

for i in range(n):
    for j in range( n):
        if i != j:
            print("columns in file " + str(i) + " not in file " + str(j) + ": " + str(columnSets[i] - columnSets[j]))

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

def union(lists, ret=[]):
    if len(lists) == 0:
        return ret
    else:
        head, *tail = lists
        return union(tail, ret + [column for column in head if column not in ret])
    
for column in union(columnss):
    print("column:", column)
    dfcs = [df[column] if column in df else None for df in dfs]

    vcs = [dfc.value_counts().sort_index() if dfc is not None else None for dfc in dfcs]

    if any((vc.count() > 10 if vc is not None else False for vc in vcs)):
        vcs = [dfc.describe() if dfc is not None else None for dfc in dfcs]

    indicess = [list(vc.index.values) for vc in vcs if vc is not None]

    print(tabulate([[index] + [vc.get(index) if vc is not None else None for vc in vcs] for index in merge(indicess)]))
            

