import os
import sys
import subprocess
from timeit import default_timer as timer
import sys
from utils import submit

dir = sys.argv[1]
patient_dimension = sys.argv[2]
patient_series = sys.argv[3]
output_dir = sys.argv[4]
cache_dir = sys.argv[5]
host_name = sys.argv[6]

submit(host_name, cache_dir, "datatrans.PreprocPerPatSeries",
       "--patient_dimension={0}/{1}".format(dir, patient_dimension),
       "--input_directory=" + dir + "/" + patient_series,
       "--output_prefix=" + output_dir + "/")

