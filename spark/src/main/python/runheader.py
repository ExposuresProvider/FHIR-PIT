import os
import sys
import subprocess
from timeit import default_timer as timer
import sys

dir = sys.argv[1]
cache_dir = sys.argv[2]
host_name = sys.argv[3]

start = timer()
cmd = ["spark-submit",
       "--master",
       "spark://{0}:7077".format(host_name),
       "--jars",
       cache_dir + "/.ivy2/cache/com.github.scopt/scopt_2.11/jars/scopt_2.11-3.7.0.jar," +
       cache_dir + "/.ivy2/cache/com.typesafe.play/play-json_2.11/jars/play-json_2.11-2.6.7.jar," +
       cache_dir + "/.ivy2/cache/com.typesafe.play/play-functional_2.11/jars/play-functional_2.11-2.6.7.jar," +
       cache_dir + "/.ivy2/cache/org.locationtech.geotrellis/geotrellis-proj4_2.11/jars/geotrellis-proj4_2.11-1.1.0.jar",
       "--class",
       "datatrans.GetColumnsDistinctValues",
       "target/scala-2.11/preproc_2.11-1.0.jar",
       "--tables=" + dir + "/observation_fact.csv," + dir + "/visit_dimension.csv",
       "--columns=concept_cd,inout_cd",
       "--output_file=" + dir + "/json/header"]
proc = subprocess.Popen(cmd)
err = proc.wait()
if err:
    print("error:", err)
end = timer()
print(end - start)