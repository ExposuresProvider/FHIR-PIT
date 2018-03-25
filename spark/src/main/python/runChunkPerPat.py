import subprocess
from timeit import default_timer as timer
import sys

input_file = sys.argv[1]
dir = sys.argv[2]
cache_dir = sys.argv[3]
host_name = sys.argv[4]

start = timer()
cmd = ["spark-submit",
       "--master",
       "spark://{0}:7077".format(host_name),
       "--jars",
       cache_dir + "/.ivy2/cache/com.github.scopt/scopt_2.11/jars/scopt_2.11-3.7.0.jar," +
       cache_dir + "/.ivy2/cache/org.locationtech.geotrellis/geotrellis-proj4_2.11/jars/geotrellis-proj4_2.11-1.1.0.jar",
       "--class",
       "datatrans.ChunkPerPat",
       "target/scala-2.11/preproc_2.11-1.0.jar",
       "{0}/{1}.csv".format(dir, input),
       "{0}/patient_series/{1}".format(dir, input_file)]
proc = subprocess.Popen(cmd)
err = proc.wait()
if err:
    print("error:", err)
end = timer()
print(end - start)


