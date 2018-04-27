import sys
from utils import merge

output_dir = sys.argv[1]
input_dirs = sys.argv[2::4]
pats = sys.argv[3::4]
hows = sys.argv[4::4]
def parsecols(x):
    if x == "":
        return []
    else:
        return x.split(",")
ons = list(map(parsecols, sys.argv[5::4]))


merge(input_dirs, pats, hows, ons, output_dir, 30)