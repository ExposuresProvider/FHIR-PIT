import sys
from utils import merge

output_dir = sys.argv[1]
input_dirs = sys.argv[2::4]
pats = sys.argv[3::4]
how = sys.argv[4::4]
on = list(map(lambda x : x.split(","), sys.argv[5::4]))


merge(input_dirs, pats, how, on, output_dir, 30)