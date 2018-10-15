import os
import sys
from utils import submit

cache_dir = sys.argv[1]
host_name = sys.argv[2]
time_series = sys.argv[3]
environmental_data = sys.argv[4]
output_dir = sys.argv[5]


submit(host_name, cache_dir, "datatrans.PreprocPerPatSeriesNearestRoad",
           "--patgeo_data=" + time_series,
           "--output_file=" + output_dir,
           "--nearestroad_data=" + environmental_data, *sys.argv[6:])

