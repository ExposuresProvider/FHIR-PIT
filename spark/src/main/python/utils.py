import subprocess
import glob
import os.path
from functools import reduce

import pandas as pd
from timeit import default_timer as timer

def submit(host_name, cache_dir, cls, *args, **kwargs):
    start = timer()
    cmd = ["spark-submit",
           "--master",
           "spark://{0}:7077".format(host_name),
           "--executor-memory",
           "140g",
           "--driver-memory",
           "64g",
           "--num-executors",
           "1",
           "--executor-cores",
           "30",
           "--jars",
           cache_dir + "/.ivy2/cache/com.github.scopt/scopt_2.11/jars/scopt_2.11-3.7.0.jar," +
           cache_dir + "/.ivy2/cache/com.typesafe.play/play-json_2.11/jars/play-json_2.11-2.6.7.jar," +
           cache_dir + "/.ivy2/cache/com.typesafe.play/play-functional_2.11/jars/play-functional_2.11-2.6.7.jar," +
           cache_dir + "/.ivy2/cache/org.locationtech.geotrellis/geotrellis-proj4_2.11/jars/geotrellis-proj4_2.11-1.1.0.jar",
           "--class",
           cls,
           "target/scala-2.11/preproc_2.11-1.0.jar"] + list(args)
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

def concat(dir, output_file, filename_column, default_value):
    dfs = []
    count = 0
    common_columns = pd.Index([])
    for file in glob.glob(dir):
        count += 1
        print("loading " + str(count) + " " + file)
        df2 = pd.read_csv(file,sep="!")
        dfs.append((file, df2))
        common_columns = common_columns.union(df2.columns)

    print("merged columns: ", common_columns.tolist())
    df1 = pd.DataFrame(columns = common_columns)
    count = 0
    # https://stackoverflow.com/questions/29929639/when-combining-pandas-dataframe-concat-or-append-can-i-set-the-default-value
    for file, df2 in dfs:
        count += 1
        print("reindexing " + str(count) + " " + file)
        df2 = df2.reindex(columns=common_columns, fill_value=default_value)
        df2[filename_column] = os.path.basename(file)
        df1 = pd.concat([df1, df2], axis=0, ignore_index=True)

    df1.to_csv(output_file, sep="!", index=False)

def merge(input_dirs, pats, output_dir):
    a = map(lambda pat : re.compile(pat + "|start_date"), pats)

    for f in glob.glob(input_dirs[0] + "/*"):
        bn = os.path.basename(f)
        print("processing", f)

        dfs = []
        for dir, a in zip(input_dirs, a):
            f = dir + "/" + bn
            df = pd.read_csv(f, sep="!")
            df3 = df[list(filter(lambda x : a.fullmatch(x), df.columns))]
            dfs.append(df3)

        dfo = reduce(lambda a, b : a.merge(b), dfs)
        dfo.to_csv(output_dir + "/" + bn, sep="!", index=False)
