import sys
from utils import merge

output_dir = sys.argv[1]
input_dirs = sys.argv[2::2]
pats = sys.argv[3::2]

merge(input_dirs, pats, output_dir, 30)