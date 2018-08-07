import os
import sys
from utils import submit

cache_dir = sys.argv[1]
host_name = sys.argv[2]
input_dir = sys.argv[3]
time_series = sys.argv[4]
patient_dimension = sys.argv[5]
environmental_data = sys.argv[6]
start_date = sys.argv[7]
end_date = sys.argv[8]
output_dir = sys.argv[9]


submit(host_name, cache_dir, "datatrans.PreprocPerPatSeriesEnvData",
           "--patient_dimension={0}".format(patient_dimension),
           "--input_directory=" + input_dir + "/",
           "--time_series=" + time_series,
           "--output_prefix=" + output_dir + "/",
           "--environmental_data=" + environmental_data,
           "--start_date=" + start_date,
           "--end_date=" + end_date, *sys.argv[10:])

