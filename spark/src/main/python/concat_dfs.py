import sys
from utils import merge

dir = sys.argv[1]

output_file = sys.argv[2]

default_value = sys.argv[3]

merge(dir, output_file, default_value)