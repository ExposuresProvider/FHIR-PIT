import sys

from utils import submit

input_file = sys.argv[1]
dir = sys.argv[2]
cache_dir = sys.argv[3]
host_name = sys.argv[4]

submit(host_name, cache_dir, "datatrans.ChunkPerPat", "{0}/{1}.csv".format(dir, input_file), "{0}/patient_series/{1}".format(dir, input_file))


