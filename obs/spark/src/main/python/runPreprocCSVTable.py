import os
import sys
from utils import submit

cache_dir = sys.argv[1]
host_name = sys.argv[2]
patient_dir = sys.argv[3]
environment_dir = sys.argv[4]
input_files = sys.argv[5]
deidentify = sys.argv[6]
output_dir = sys.argv[7]


submit(host_name, cache_dir, "datatrans.PreprocCSVTable",
           "--patient_directory=" + patient_dir,
           "--environment_directory=" + environment_dir,
           "--input_files=" + input_files,
       "--deidentify=" + deidentify,
           "--output_directory=" + output_dir, *sys.argv[8:])

