import subprocess
import os.path
import re

from timeit import default_timer as timer
import os

def submit(host_name, cls, *args, **kwargs):
    start = timer()

    if host_name == "local":
        submit_2_target_nodet = "local[*]"
    else:
        submit_2_target_nodet = "spark://{0}:7077".format(host_name)

    cmd = ["spark-submit",
           "--master",
           submit_2_target_nodet,
           "--executor-memory",
           "140g",
           "--driver-memory",
           "64g",
           "--num-executors",
           "1",
           "--executor-cores",
           "30",
           "--class",
           cls,
           "target/scala-2.11/Preproc-assembly-1.0.jar"] + list(args)
    print(cmd)
    if "log" in kwargs and "log2" in kwargs:
        log = kwargs["log"]
        log2 = kwargs["log2"]
        with open(log, "w") as file:
            with open(log2, "w") as file2:
                proc = subprocess.Popen(cmd, stdout=file, stderr=file2)
                err = proc.wait()
    else:
        proc = subprocess.Popen(cmd)
        err = proc.wait()
    if err:
        print("error:", err)
    end = timer()
    print(end - start)


