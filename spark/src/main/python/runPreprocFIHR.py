import sys
from utils import submit

cache_dir = sys.argv[1]
host_name = sys.argv[2]
input_dir = sys.argv[3]
resc_types = sys.argv[4]
skip_preproc = sys.argv[5]
output_dir = sys.argv[6]

args = []
args.extend(sys.argv[7:])

submit(host_name, cache_dir, "datatrans.PreprocFIHR",
           "--input_dir=" + input_dir,
           "--resc_types=" + resc_types,
           "--skip_preproc=" + skip_preproc,
           "--output_dir=" + output_dir, *args)

