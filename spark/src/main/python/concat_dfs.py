import sys
from utils import concat

dir = sys.argv[1]

output_file = sys.argv[2]

filename_column = sys.argv[3]

if filename_column == "":
    filename_column = None

default_value = sys.argv[4]

sep = sys.argv[5]

distinct = sys.argv[6] == "distinct"

concat(dir, output_file, filename_column, default_value, sep, distinct)