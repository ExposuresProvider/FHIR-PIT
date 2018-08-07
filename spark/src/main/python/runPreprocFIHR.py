import sys
from utils import submit

cache_dir = sys.argv[1]
host_name = sys.argv[2]
patient_dimension = sys.argv[3]
resc_types = sys.argv[4]
output_dir = sys.argv[5]


submit(host_name, cache_dir, "datatrans.PreprocFIHR",
           "--input_dir={0}".format(patient_dimension),
           "--resc_types=" + resc_types,
           "--output_dir=" + output_dir, *sys.argv[6:])

