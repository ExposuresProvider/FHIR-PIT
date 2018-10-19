import os
import sys
from utils import submit

cache_dir = sys.argv[1]
host_name = sys.argv[2]
input_files = sys.argv[3]
output_file = sys.argv[4]


submit(host_name, cache_dir, "datatrans.PreprocCSVTable",
           "--input_files=" + input_files,
           "--output_file=" + output_file, *sys.argv[5:])

