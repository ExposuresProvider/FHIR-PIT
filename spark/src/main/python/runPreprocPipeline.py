import sys
import subprocess
import os
import tempfile

host_name, driver_memory, executor_memory, config = sys.argv[1:]

tf = tempfile.NamedTemporaryFile(delete=False)
tf.close()

tfname = tf.name

# subprocess.call(["dhall-to-yaml", "--file", config, "--output", tfname])

def submit(host_name, cls, driver_mem, exec_mem, *args, **kwargs):
    if host_name == "local":
        submit_2_target_nodet = "local[*]"
    else:
        submit_2_target_nodet = "spark://{0}:7077".format(host_name)

    cmd = ["spark-submit",
           "--master",
           submit_2_target_nodet,
           "--executor-memory",
           exec_mem,
           "--driver-memory",
           driver_mem,
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

submit(host_name, "datatrans.PreprocPipeline", driver_memory, executor_memory, "--config=" + config)

os.unlink(tfname)

