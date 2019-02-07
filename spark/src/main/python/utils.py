import subprocess
import glob
import os.path
import re
import joblib

import pandas as pd
from timeit import default_timer as timer
import os

def submit(host_name, cache_dir, cls, *args, **kwargs):
    start = timer()

    cache = ",".join([filename for filename in glob.iglob(os.path.join(cache_dir, ".ivy2", "**", "*.jar"), recursive=True)])

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
           "--jars"] + [cache] + [
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


def run(cache_dir, cls, *args, **kwargs):
    start = timer()

    cache = ":".join(["target/scala-2.11/preproc_2.11-1.0.jar"] + [filename for filename in glob.iglob(os.path.join(cache_dir, ".ivy2", "**", "*.jar"), recursive=True)])

    cmd = ["java",
           "-classpath",
           cache,
           cls] + list(args)
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


def concat(dir, output_file, column_pattern, filename_column, default_value, sep, distinct):
    regular_expression = re.compile(column_pattern)
    dfs = []
    count = 0
    common_columns = pd.Index([])
    for file in glob.glob(dir):
        count += 1
        print("loading " + str(count) + " " + file)
        df2 = pd.read_csv(file,sep=sep)
        dfs.append((file, df2))
        common_columns = common_columns.union(df2.columns)

    columns_match_column_pattern = list(filter(lambda x : regular_expression.fullmatch(x), common_columns))

    print("merged columns: ", columns_match_column_pattern)
    df1 = pd.DataFrame(columns = columns_match_column_pattern)
    count = 0
    reindxed_dfs = []
    # https://stackoverflow.com/questions/29929639/when-combining-pandas-dataframe-concat-or-append-can-i-set-the-default-value
    for file, df2 in dfs:
        count += 1
        print("reindexing " + str(count) + " " + file)
        df2 = df2.reindex(columns=columns_match_column_pattern, fill_value=default_value)
        if filename_column is not None:
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

    combined_df = combined_dfs[0]
    if distinct:
        combined_df = combined_df.drop_duplicates()

    combined_df.to_csv(output_file, sep=sep, index=False)

def proc_pid(input_dirs, in_seps, column_patterns, hows, ons, output_dir, f, out_sep):
    basename_f = os.path.basename(f)

    dfs = []
    for dir, in_sep, column_pattern in zip(input_dirs, in_seps, column_patterns):
        input_f = dir + "/" + basename_f
        if os.path.exists(input_f):
            print("processing " + input_f)
            df = pd.read_csv(input_f, sep=in_sep)
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
        df_out.to_csv(output_f, sep=out_sep, index=False)
    else:
        print("empty " + output_f)

def merge(input_dirs, in_seps, pats, hows, ons, output_dir, out_sep, n_jobs):
    regular_expressions = list(map(lambda pat : re.compile(pat), pats))
    files = glob.glob(input_dirs[0] + "/*")


    joblib.Parallel(n_jobs = n_jobs)(joblib.delayed(proc_pid)(input_dirs, in_seps, regular_expressions, hows, ons, output_dir, f, out_sep) for f in files)
