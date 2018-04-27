import sys
from utils import merge

output_dir = sys.argv[1]
input_dirs = sys.argv[2::4]
pats = sys.argv[3::4]
how = sys.argv[4::4]
on = map(lambda x : x.split(","), sys.argv[5::4])


merge(input_dirs, pats, output_dir, 30, how, on)