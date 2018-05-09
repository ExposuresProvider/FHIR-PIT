import sys
from utils import concat

dir = sys.argv[1]

output_file = sys.argv[2]

filename_column = sys.argv[3]

default_value = sys.argv[4]

sep = sys.argv[5]

concat(dir, output_file, filename_column, default_value, sep)