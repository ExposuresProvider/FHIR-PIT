import sys
from utils import submit

dir = sys.argv[1]
cache_dir = sys.argv[2]
host_name = sys.argv[3]

submit(host_name, cache_dir, "datatrans.GetColumnsDistinctValues",
       "--tables=" + dir + "/observation_fact.csv," + dir + "/visit_dimension.csv",
       "--columns=concept_cd,inout_cd",
       "--output_file=" + dir + "/json/header")

