import sys
from utils import submit

host_name, input_dir, resc_types, skip_preproc, output_dir, *args = sys.argv[1:]

 = []
args.extend(sys.argv[6:])

submit(host_name, cache_dir, "datatrans.PreprocFIHR",
           "--input_dir=" + input_dir,
           "--resc_types=" + resc_types,
           "--skip_preproc=" + skip_preproc,
           "--output_dir=" + output_dir, *args)

