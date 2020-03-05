import sys
from utils import submit

cache_dir = sys.argv[1]
host_name = sys.argv[2]
dir = sys.argv[3]
year = sys.argv[4]

submit(host_name, cache_dir, "datatrans.PreprocDailyEnvData",
       "--input_directory={0}/cmaq{1}".format(dir, year),
       "--output_prefix={0}/cmaq{1}/".format(dir, year))


