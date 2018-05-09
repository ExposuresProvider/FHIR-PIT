import sys
from utils import concat

dir = sys.argv[1]

output_file = sys.argv[2]

column_pattern = sys.argv[3]

filename_column = sys.argv[4]

if filename_column == "":
    filename_column = None

default_value = sys.argv[5]

sep = sys.argv[6]

distinct = len(sys.argv) == 8 and sys.argv[7] == "distinct"

concat(dir, output_file, filename_column, default_value, sep, distinct)