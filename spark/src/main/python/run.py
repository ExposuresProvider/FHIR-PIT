import sys
import subprocess
from timeit import default_timer as timer

host_name = sys.argv[1]
cls = sys.argv[2]
cache_dir = "/opt/RENCI/scripts/"

args = sys.argv[3:]

start = timer()
cmd = ["spark-submit",
       "--master",
       "spark://{0}:7077".format(host_name),
       "--jars",
       cache_dir + "scopt_2.11-3.7.0.jar," +
       cache_dir + "play-json_2.11-2.6.7.jar," +
       cache_dir + "play-functional_2.11-2.6.7.jar," +
       cache_dir + "geotrellis-proj4_2.11-1.1.0.jar",
       "--class",
       cls,
       cache_dir + "preproc_2.11-1.0.jar"] + list(args)
proc = subprocess.Popen(cmd)
err = proc.wait()
if err:
    print("error:", err)
end = timer()
print(end - start)