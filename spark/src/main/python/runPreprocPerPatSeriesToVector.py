import os
import sys
from utils import submit

cache_dir = sys.argv[1]
host_name = sys.argv[2]
input_dir = sys.argv[3]
med_map = sys.argv[4]
output_dir = sys.argv[5]


submit(host_name, cache_dir, "datatrans.PreprocPerPatSeriesToVector",
       "--input_directory=" + input_dir,
       "--output_directory=" + output_dir,
       "--mdctn_rxnorm=" + med_map,
       *sys.argv[6:])

