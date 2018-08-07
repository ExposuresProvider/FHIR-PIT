import os
import sys
from utils import submit

cache_dir = sys.argv[1]
host_name = sys.argv[2]
patient_dimension = sys.argv[3]
time_series = sys.argv[4]
environmental_data = sys.argv[5]
acs_data = sys.argv[6]
output_dir = sys.argv[7]


submit(host_name, cache_dir, "datatrans.PreprocPerPatSeriesACS",
           "--patient_dimension={0}".format(patient_dimension),
           "--time_series=" + time_series,
           "--output_file=" + output_dir,
           "--geoid_data=" + environmental_data,
           "--acs_data=" + acs_data, *sys.argv[8:])

