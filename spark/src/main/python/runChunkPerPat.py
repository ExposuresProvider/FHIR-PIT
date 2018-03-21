import os
import sys
import subprocess
from timeit import default_timer as timer
import sys

dir = sys.argv[1]
cache_dir = sys.argv[2]

start = timer()
cmd = ["spark-submit",
       "--master",
       "spark://a-HP-Z820-Workstation:7077",
       "--jars",
       cache_dir + "/.ivy2/cache/com.github.scopt/scopt_2.11/jars/scopt_2.11-3.7.0.jar," +
       cache_dir + "/.ivy2/cache/org.locationtech.geotrellis/geotrellis-proj4_2.11/jars/geotrellis-proj4_2.11-1.1.0.jar",
       "--class",
       "datatrans.ChunkPerPat",
       "target/scala-2.11/preproc_2.11-1.0.jar",
       dir + "/patient_dimension.csv",
       dir + "/visit_dimension.csv",
       dir + "/observation_fact.csv"
       dir + "/patient_series/"]
proc = subprocess.Popen(cmd)
err = proc.wait()
if err:
    print("error:", err)
end = timer()
print(end - start)


