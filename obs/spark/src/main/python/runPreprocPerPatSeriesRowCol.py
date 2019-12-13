import os
import sys
from utils import submit

cache_dir = sys.argv[1]
host_name = sys.argv[2]
patient_dimension = sys.argv[3]
time_series = sys.argv[4]
year = sys.argv[5]
output_dir = sys.argv[6]


submit(host_name, cache_dir, "datatrans.PreprocPerPatSeriesRowCol",
           "--patient_dimension={0}".format(patient_dimension),
           "--time_series=" + time_series,
           "--output_file=" + output_dir,
           "--year=" + year, *sys.argv[7:])

