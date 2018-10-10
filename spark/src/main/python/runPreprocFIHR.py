import sys
from utils import submit

cache_dir = sys.argv[1]
host_name = sys.argv[2]
input_dir = sys.argv[3]
resc_types = sys.argv[4]
skip_preproc = sys.argv[5]
replace_pat = bool(sys.argv[6])
output_dir = sys.argv[7]

args = []
if replace_pat:
    args.append("--replace_pat")
args.extend(sys.argv[8:])

submit(host_name, cache_dir, "datatrans.PreprocFIHR",
           "--input_dir={0}".format(patient_dimension),
           "--resc_types=" + resc_types,
           "--skip_preproc" + skip_preproc,
           "--output_dir=" + output_dir, *args)

