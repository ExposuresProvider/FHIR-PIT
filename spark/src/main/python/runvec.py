import os
import sys
import subprocess
from timeit import default_timer as timer

def process_pids(pids):
    pids0 = ",".join(pids)
    start = timer()
    cmd = ["/mnt/d/spark-2.3.0-bin-hadoop2.7/bin/spark-submit",
           "--master",
           "spark://a-HP-Z820-Workstation:7077",
           "--jars",
           "/home/a/.ivy2/cache/com.github.scopt/scopt_2.11/jars/scopt_2.11-3.7.0.jar,/home/a/.ivy2/cache/com.typesafe.play/play-json_2.11/jars/play-json_2.11-2.6.7.jar,/home/a/.ivy2/cache/com.typesafe.play/play-functional_2.11/jars/play-functional_2.11-2.6.7.jar",
           "--class",
           "datatrans.PreprocPerPatSeriesToVector",
           "target/scala-2.11/preproc_2.11-1.0.jar",
           "--patient_num_list={0}".format(pids0),
           "--input_directory=/mnt/d/json",
           "--output_prefix=/mnt/d/json/vector"]
    log = "/mnt/d/json/stdoutvec" + pids0
    log2 = "/mnt/d/json/stderrvec" + pids0
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
        if os.path.exists("/mnt/d/json/vector"+pid):
            print(pid + " exists")
        else:
            pids.append(pid)
            if len(pids) == count:
                process_pids(pids)
                pids.clear()
        print("processed", n)

    if len(pids) != 0:
        process_pids(pids)
