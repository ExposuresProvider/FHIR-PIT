import sys
import os
from datetime import datetime, timedelta
import yaml
import pandas as pd
import numpy as np
import progressbar


statisticsFunctions = {
    "min": np.min,
    "stddev": np.std,
    "max": np.max,
    "avg": np.mean
}

fmt = "%Y-%m-%d"

def step(config):
    input_dir = config["input_dir"]
    output_dir = config["output_dir"]
    
    indices = config["indices"]
    
    statistics = config["statistics"]

    for filename in progressbar.progressbar(os.listdir(input_dir), redirect_stdout=True):
        df = pd.read_csv(f"{input_dir}/{filename}")[["start_date"] + indices]
        df["year"] = df["start_date"].apply(lambda date: date[0:4])
        
        df_agg = df.groupby(["year"]).agg(**{
            f"{index}_{statistic}" : (index, statisticsFunctions[statistic]) for index in indices for statistic in statistics
        })
        
        df["next_date"] = df["start_date"].apply(lambda date: (datetime.strptime(date, fmt) + timedelta(days=1)).strftime(fmt))
        df_prev_date = df[["next_date"] + indices]
        df_prev_date.columns = ["start_date"] + list(map(lambda x: x + "_prev_date", indices))
        
        df2 = df.merge(df_agg, how="left", on="year").merge(df_prev_date, how="left", on="start_date")
        df2.to_csv(f"{output_dir}/{filename}", index=False)

if __name__ == "__main__":
    config = yaml.load(sys.argv[1])
    step(config)
