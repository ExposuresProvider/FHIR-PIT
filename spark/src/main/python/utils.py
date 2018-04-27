import subprocess
import glob
import os.path
import re
import joblib

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
    reindxed_dfs = []
    # https://stackoverflow.com/questions/29929639/when-combining-pandas-dataframe-concat-or-append-can-i-set-the-default-value
    for file, df2 in dfs:
        count += 1
        print("reindexing " + str(count) + " " + file)
        df2 = df2.reindex(columns=common_columns, fill_value=default_value)
        df2[filename_column] = os.path.basename(file)
        reindxed_dfs.append(df2)

    count = 0
    combined_dfs = reindxed_dfs
    while len(combined_dfs) > 1:
        count += 1
        print("combining pass " + str(count))
        combine0 = combined_dfs[0::2]
        combine1 = combined_dfs[1::2]

        combined_dfs = []
        for df0, df1 in zip(combine0, combine1):
            print("combining " + str(len(combined_dfs) + 1))
            combined_dfs.append(pd.concat([df0, df1], axis=0, ignore_index=True))
        if len(combine0) > len(combine1):
            combined_dfs.append(combine0[-1])

    combined_dfs[0].to_csv(output_file, sep="!", index=False)

def proc_pid(input_dirs, column_patterns, hows, ons, output_dir, f):
    basename_f = os.path.basename(f)

    dfs = []
    for dir, column_pattern in zip(input_dirs, column_patterns):
        input_f = dir + "/" + basename_f
        if os.path.exists(input_f):
            print("processing " + input_f)
            df = pd.read_csv(input_f, sep="!")
            df_match_column_pattern = df[list(filter(lambda x : column_pattern.fullmatch(x), df.columns))]
            dfs.append(df_match_column_pattern)
        else:
            print(input_f + " does not exist")
            dfs.append(None)

    df_out = dfs[0]
    for i in range(len(dfs) - 1):
        how = hows[i]
        df_in = dfs[i+1]
        if df_out is None:
            if how == "left" or how == "inner":
                return
            else:
                df_out = df_in
        elif df_in is None:
            if how == "right" or how == "inner":
                return
        else:
            df_out = df_out.merge(dfs[i+1], how=hows[i], on=ons[i])

    if df_out is None:
        nrows = 0
        ncols = 0
    else:
        nrows = len(df_out.index)
        ncols = len(df_out.columns)
    output_f = output_dir + "/" + basename_f
    if nrows != 0 and ncols != 0:
        print(str(nrows) + " rows " + str(ncols) + " cols " + output_f)
        df_out.to_csv(output_f, sep="!", index=False)
    else:
        print("empty " + output_f)

def merge(input_dirs, pats, hows, ons, output_dir, n_jobs):
    regular_expressions = list(map(lambda pat : re.compile(pat), pats))
    files = glob.glob(input_dirs[0] + "/*")


    joblib.Parallel(n_jobs = n_jobs)(joblib.delayed(proc_pid)(input_dirs, regular_expressions, hows, ons, output_dir, f) for f in files)
