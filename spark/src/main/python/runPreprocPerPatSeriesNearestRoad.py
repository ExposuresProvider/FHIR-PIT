import os
import sys
from utils import submit

cache_dir = sys.argv[1]
host_name = sys.argv[2]
input_dir = sys.argv[3]
patient_dimension = sys.argv[4]
time_series = sys.argv[5]
environmental_data = sys.argv[6]
output_dir = sys.argv[7]


submit(host_name, cache_dir, "datatrans.PreprocPerPatSeriesNearestRoad",
           "--patient_dimension={0}".format(patient_dimension),
           "--input_directory=" + input_dir + "/",
           "--time_series=" + time_series,
           "--output_file=" + output_dir,
           "--nearestroad_data=" + environmental_data, *sys.argv[8:])

