import os
import sys
import subprocess
from timeit import default_timer as timer
import sys
from utils import submit

dir = sys.argv[1]
cache_dir = sys.argv[2]
host_name = sys.argv[3]

submit(host_name, cache_dir, "datatrans.PreprocPerPatSeries",
       "--patient_dimension={0}/patient_dimension.csv".format(dir),
       "--input_directory=" + dir + "/patient_series",
       "--output_prefix=" + dir + "/json/")

