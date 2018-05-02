import os
import sys
from utils import submit

input_dir = sys.argv[1]
time_series = sys.argv[2]
patient_dimension = sys.argv[3]
environmental_data = sys.argv[4]
output_dir = sys.argv[5]
cache_dir = sys.argv[6]
host_name = sys.argv[7]


submit(host_name, cache_dir, "datatrans.PreprocPerPatSeriesNearestRoad",
           "--patient_dimension={0}".format(patient_dimension),
           "--input_directory=" + input_dir + "/",
           "--time_series=" + time_series,
           "--output_file=" + output_dir,
           "--nearestroad_data=" + environmental_data, *sys.argv[8:])

