import sys
from submit import submit
import subprocess
import os
import tempfile

host_name, config = sys.argv[1:]

tf = tempfile.NamedTemporaryFile(delete=False)
tf.close()

tfname = tf.name

# subprocess.call(["dhall-to-yaml", "--file", config, "--output", tfname])

def submit(host_name, cls, *args, **kwargs):
    if host_name == "local":
        submit_2_target_nodet = "local[*]"
    else:
        submit_2_target_nodet = "spark://{0}:7077".format(host_name)

    cmd = ["/home/jjgarcia/spark-2.4.7-bin-hadoop2.7/bin/spark-submit",
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

submit(host_name, "datatrans.PreprocPipeline", "--config=" + config)

os.unlink(tfname)

