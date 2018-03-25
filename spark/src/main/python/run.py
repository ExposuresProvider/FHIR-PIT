import os
import sys
import subprocess
from timeit import default_timer as timer
import sys
from utils import submit

dir = sys.argv[3]
cache_dir = sys.argv[4]
host_name = sys.argv[5]

def process_pids(pids):
    pids0 = ",".join(pids)

    submit(host_name, cache_dir, "datatrans.PreprocPerPatSeries",
           "--patient_num_list={0}".format(pids0),
           "--input_directory=" + dir + "/patient_series",
           "--output_prefix=" + dir + "/json/",
           log = dir + "/json/stdout" + pids0,
           log2 = dir + "/json/stderr" + pids0)

with open(sys.argv[1]) as f:
    count = int(sys.argv[2])
    pids = []
    n = 0
    for line in f.readlines():
        n += 1
        pid = line.rstrip("\n")
        print("processing", pid)
        if os.path.exists(dir + "/json/"+pid):
            print(pid + " exists")
        else:
            pids.append(pid)
            if len(pids) == count:
                process_pids(pids)
                pids.clear()
        print("processed", n)

    if len(pids) != 0:
        process_pids(pids)
