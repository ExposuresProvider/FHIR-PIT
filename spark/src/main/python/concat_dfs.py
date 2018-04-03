import sys
from utils import merge

dir = sys.argv[1]

output_file = sys.argv[2]

filename_column = sys.argv[3]

default_value = sys.argv[4]

merge(dir, output_file, filename_column, default_value)