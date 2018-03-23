import os
import sys
import subprocess
from timeit import default_timer as timer
import sys

dir = sys.argv[3]
cache_dir = sys.argv[4]
host_name = sys.argv[5]

def process_pids(pids):
    pids0 = ",".join(pids)
    start = timer()
    cmd = ["spark-submit",
           "--master",
           "spark://{0}:7077".format(host_name),
           "--jars",
           cache_dir + "/.ivy2/cache/com.github.scopt/scopt_2.11/jars/scopt_2.11-3.7.0.jar," +
           cache_dir + "/.ivy2/cache/org.locationtech.geotrellis/geotrellis-proj4_2.11/jars/geotrellis-proj4_2.11-1.1.0.jar",
           "--class",
           "datatrans.PreprocPerPatSeries",
           "target/scala-2.11/preproc_2.11-1.0.jar",
           "--patient_num_list={0}".format(pids0),
           "--input_directory=" + dir + "/patient_series",
           "--output_prefix=" + dir + "/json/"]
    log = dir + "/json/stdout" + pids0
    log2 = dir + "/json/stderr" + pids0
    with open(log, "w") as file:
        with open(log2, "w") as file2:
            proc = subprocess.Popen(cmd, stdout=file, stderr=file2)
            err = proc.wait()
            if err:
                print("error:", err)
    end = timer()
    print(end - start)


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
