import os
import pandas as pd
import numpy as np
import os.path
from joblib import Parallel, delayed
from tqdm import tqdm
from stepUtils import tqdm_joblib
import argparse
from dateutil.parser import *
from datetime import datetime, timedelta
from dateutil.tz import *
import functools
import yaml


def join_env(pdf, env_fn):
    if os.path.isfile(env_fn):
        envdf = pd.read_csv(env_fn)
        penvdf = pdf.merge(envdf, on="start_date", how="left")
    else:
        penvdf = pdf
    return penvdf

def extractYear(x):
    return int(x[0:4])


def proc_pid(config, p):
        # print(f"processing {p}")
        v = config["patient_file"]
        env_fn = config["environment_file"]
        env2_fn = config["environment2_file"]
        input_files = config["input_files"]
        output_dir = config["output_dir"]
        offset_hours = config["offset_hours"]
        start_date_str = config["start_date"]
        end_date_str = config["end_date"]
        start_date = parseTimestamp(start_date_str, offset_hours)
        end_date = parseTimestamp(end_date_str, offset_hours)
        start_year = start_date.year
        end_year = end_date.year
        years = range(start_year, end_year)

        input_dfs = list(map(lambda nr: pd.read_csv(nr), input_files))
        sdf = functools.reduce(lambda l,r: l.merge(r, on="patient_num", how="outer"), input_dfs) if len(input_dfs) != 0 else None

        fn = f"{v}/{p}.csv"
        pdf = pd.read_csv(fn)

        penvdf = join_env(pdf, f"{env_fn}/{p}")
        penv2df = join_env(penvdf, f"{env2_fn}/{p}")

        penv2df["year"] = penv2df["start_date"].apply(extractYear)

        padf = penv2df.merge(sdf, on="patient_num", how="left") if sdf is not None else penv2df
        for year in years:
            output_dir = f"/var/fhir/icees/{year}/per_patient"
            os.makedirs(output_dir, exist_ok=True)
            padf[padf["year"] == year].to_csv(f"{output_dir}/{p}", index=False)


def parseTimestamp(a, offset_hours):
    dt = parse(a)
    tz = tzoffset(None, timedelta(hours=offset_hours))
    dtl = dt.astimezone(tz)
    return dtl


def step(params, config):
    v = config["patient_file"]
    files = list(os.listdir(v))

    with tqdm_joblib(tqdm(total=len(files))) as progress_bar:
        Parallel(n_jobs=params["n_jobs"])(delayed(proc_pid)(config, f[:-4]) for f in files)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--n_jobs', metavar='N', type=int, default=16, help='an integer for the accumulator')
    parser.add_argument('--config', type=str, required=True, help='sum the integers (default: find the max)')

    args = parser.parse_args()
    with open(args.config) as f:
        config = yaml.safe_load(f)
    step({"n_jobs": args.n_jobs}, config)
