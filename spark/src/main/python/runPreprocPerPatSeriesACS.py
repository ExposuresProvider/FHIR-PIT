import os
import sys
from utils import submit

input_dir = sys.argv[1]
time_series = sys.argv[2]
patient_dimension = sys.argv[3]
environmental_data = sys.argv[4]
acs_data = sys.argv[5]
output_dir = sys.argv[6]
cache_dir = sys.argv[7]
host_name = sys.argv[8]


submit(host_name, cache_dir, "datatrans.PreprocPerPatSeriesACS",
           "--patient_dimension={0}".format(patient_dimension),
           "--input_directory=" + input_dir + "/",
           "--time_series=" + time_series,
           "--output_file=" + output_dir + "/",
           "--geoid_data=" + environmental_data,
           "--acs_data=" + acs_data, *sys.argv[9:])

