import os
import sys
import subprocess
from timeit import default_timer as timer

start = timer()
cmd = ["/mnt/d/spark-2.3.0-bin-hadoop2.7/bin/spark-submit",
       "--master",
       "spark://a-HP-Z820-Workstation:7077",
       "--jars",
       "/home/a/.ivy2/cache/com.github.scopt/scopt_2.11/jars/scopt_2.11-3.7.0.jar,/home/a/.ivy2/cache/com.typesafe.play/play-json_2.11/jars/play-json_2.11-2.6.7.jar,/home/a/.ivy2/cache/com.typesafe.play/play-functional_2.11/jars/play-functional_2.11-2.6.7.jar",
       "--class",
       "datatrans.PreprocPerPatSeriesToHeader",
       "target/scala-2.11/preproc_2.11-1.0.jar",
       "--tables=/mnt/d/observation_fact.csv,/mnt/d/visit_dimension.csv",
       "--columns=concept_cd,inout_cd",
       "--output_file=/mnt/d/json/header"]
proc = subprocess.Popen(cmd)
err = proc.wait()
if err:
    print("error:", err)
end = timer()
print(end - start)
