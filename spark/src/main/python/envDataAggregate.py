import sys
import os
from datetime import datetime, timedelta
import yaml
import pandas as pd
import numpy as np
import contextlib
from tqdm import tqdm
from joblib import Parallel, delayed
import joblib
import argparse

statisticsFunctions = {
    "min": np.min,
    "stddev": np.std,
    "max": np.max,
    "avg": np.mean
}


fmt = "%Y-%m-%d"


def aggregateByYear(input_dir, output_dir, indices, statistics):
    def func(filename):
        df = pd.read_csv(f"{input_dir}/{filename}")[["start_date"] + indices]
        df["year"] = df["start_date"].apply(lambda date: date[0:4])
        
        df_agg = df.groupby(["year"]).agg(**{
            f"{index}_{statistic}" : (index, stat_func) for index in indices for statistic, stat_func in statistics
        })
        
        df["next_date"] = df["start_date"].apply(lambda date: (datetime.strptime(date, fmt) + timedelta(days=1)).strftime(fmt))
        df_prev_date = df[["next_date"] + indices]
        df_prev_date.columns = ["start_date"] + list(map(lambda x: x + "_prev_date", indices))
        
        df2 = df.merge(df_agg, how="left", on="year").merge(df_prev_date, how="left", on="start_date")
        df2.to_csv(f"{output_dir}/{filename}", index=False)
    return func


@contextlib.contextmanager
def tqdm_joblib(tqdm_object):
    """Context manager to patch joblib to report into tqdm progress bar given as argument"""
    class TqdmBatchCompletionCallback:
        def __init__(self, time, index, parallel):
            self.index = index
            self.parallel = parallel

        def __call__(self, index):
            tqdm_object.update()
            if self.parallel._original_iterator is not None:
                self.parallel.dispatch_next()

    old_batch_callback = joblib.parallel.BatchCompletionCallBack
    joblib.parallel.BatchCompletionCallBack = TqdmBatchCompletionCallback
    try:
        yield tqdm_object
    finally:
        joblib.parallel.BatchCompletionCallBack = old_batch_callback
        tqdm_object.close()


def step(params, config):
    input_dir = config["input_dir"]
    output_dir = config["output_dir"]
    
    indices = config["indices"]
    
    statistics = config["statistics"]

    files = list(os.listdir(input_dir))

    with tqdm_joblib(tqdm(total=len(files))) as progress_bar:
        Parallel(n_jobs=params.n_jobs)(delayed(aggregateByYear(input_dir, output_dir, indices, statistics))(filename) for filename in files)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--n_jobs', metavar='N', type=int, default=16, help='an integer for the accumulator')
    parser.add_argument('--config', type=str, required=True, help='sum the integers (default: find the max)')

    args = parser.parse_args()
    with open(args.config) as f:
        config = yaml.load(f)
    step({"n_jobs": args.n_jobs}, config)
