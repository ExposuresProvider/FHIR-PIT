import os
import sys
import subprocess
from timeit import default_timer as timer
import sys
from utils import submit

dir = sys.argv[1]
patient_dimension = sys.argv[2]
output_dir = sys.argv[3]
cache_dir = sys.argv[4]
host_name = sys.argv[5]

submit(host_name, cache_dir, "datatrans.PreprocPerPatSeriesPatientSK",
       "--patient_dimension={0}".format(patient_dimension),
       "--input_directory=" + dir,
       "--output_prefix=" + output_dir + "/")

