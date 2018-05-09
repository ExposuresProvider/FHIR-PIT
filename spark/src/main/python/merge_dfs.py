import sys
from utils import merge

output_dir = sys.argv[1]
out_sep = sys.argv[2]
input_dirs = sys.argv[3::5]
in_seps = sys.argv[4::5]
pats = sys.argv[5::5]
hows = sys.argv[6::5]
def parsecols(x):
    if x == "":
        return []
    else:
        return x.split(",")
ons = list(map(parsecols, sys.argv[7::5]))


merge(input_dirs, in_seps, pats, hows, ons, output_dir, out_sep, 30)