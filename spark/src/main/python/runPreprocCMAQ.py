import sys
from utils import submit

dir = sys.argv[1]
cache_dir = sys.argv[2]
host_name = sys.argv[3]

submit(host_name, cache_dir, "datatrans.PreprocCMAQ",
       dir + "/cmaq/cmaq2010.csv",
       dir + "/cmaq2010")


