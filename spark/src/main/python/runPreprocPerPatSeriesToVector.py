import os
import sys
from utils import submit

input_dir = sys.argv[1]
output_dir = sys.argv[2]
cache_dir = sys.argv[3]
host_name = sys.argv[4]


submit(host_name, cache_dir, "datatrans.PreprocPerPatSeriesToVector",
           "--input_directory=" + input_dir + "/",
           "--output_prefix=" + output_dir + "/",
       *sys.argv[5:])

