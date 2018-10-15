import sys
from utils import submit

cache_dir = sys.argv[1]
host_name = sys.argv[2]
time_series = sys.argv[3]
environmental_data = sys.argv[4]
start_date = sys.argv[5]
end_date = sys.argv[6]
output_dir = sys.argv[7]


submit(host_name, cache_dir, "datatrans.PreprocPerPatSeriesEnvData",
           "--patgeo_data=" + time_series",
           "--output_file=" + output_dir + "/%i",
           "--output_format=csv",
           "--environmental_data=" + environmental_data,
           "--start_date=" + start_date,
           "--end_date=" + end_date, *sys.argv[8:])

