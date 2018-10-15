import os
import sys
from utils import submit

cache_dir = sys.argv[1]
host_name = sys.argv[2]
time_series = sys.argv[3]
environmental_data = sys.argv[4]
acs_data = sys.argv[5]
output_dir = sys.argv[6]


submit(host_name, cache_dir, "datatrans.PreprocPerPatSeriesACS",
           "--clinical_data=" + time_series,
           "--output_file=" + output_dir,
           "--geoid_data=" + environmental_data,
           "--acs_data=" + acs_data, *sys.argv[7:])

