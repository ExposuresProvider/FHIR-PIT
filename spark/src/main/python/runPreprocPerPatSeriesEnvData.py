import sys
from utils import submit

cache_dir = sys.argv[1]
host_name = sys.argv[2]
time_series = sys.argv[3]
fips_data = sys.argv[4]
environmental_data = sys.argv[5]
start_date = sys.argv[6]
end_date = sys.argv[7]
output_dir = sys.argv[8]


submit(host_name, cache_dir, "datatrans.PreprocPerPatSeriesEnvData",
           "--patgeo_data=" + time_series,
           "--output_file=" + output_dir + "/%i",
           "--environmental_data=" + environmental_data,
           "--fips_data=" + fips_data,
           "--start_date=" + start_date,
           "--end_date=" + end_date, *sys.argv[9:])

