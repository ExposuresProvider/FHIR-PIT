import sys
from utils import submit

cache_dir = sys.argv[1]
host_name = sys.argv[2]
input_file = sys.argv[3]
output_dir = sys.argv[4]

submit(host_name, cache_dir, "datatrans.PreprocCMAQ",
       input_file,
       output_dir)


