import os
import sys
from utils import submit

cache_dir = sys.argv[1]
host_name = sys.argv[2]
patient_dimension = sys.argv[3]
time_series = sys.argv[4]
input_resc_dir = sys.argv[5]
resources = sys.argv[6]
output_dir = sys.argv[7]


submit(host_name, cache_dir, "datatrans.PreprocCombineData",
           "--patient_dimension={0}".format(patient_dimension),
           "--time_series=" + time_series,
           "--input_resc_dir=" + input_resc_dir,
           "--resources=" + resources,
           "--output_dir=" + output_dir, *sys.argv[8:])

