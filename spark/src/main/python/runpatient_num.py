import sys
from utils import submit

dir = sys.argv[1]
cache_dir = sys.argv[2]
host_name = sys.argv[3]

submit(host_name, cache_dir, "datatrans.GetColumnsDistinctValues",
       "--tables=" + dir + "/patient_dimension.csv",
       "--columns=patient_num",
       "--output_file=" + dir + "/json/patient_num")
