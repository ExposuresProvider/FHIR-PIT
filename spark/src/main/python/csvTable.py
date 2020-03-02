import os
import pandas as pd
import numpy as np
import os.path
from progressbar import progressbar

nr = "/var/fhir/other_processed/nearestroad.csv"

nr2 = "/var/fhir/other_processed/nearestroad2.csv"

acs = "/var/fhir/other_processed/acs.csv"

acs2 = "/var/fhir/other_processed/acs2.csv"

nrdf = pd.read_csv(nr)
nr2df = pd.read_csv(nr2)
acsdf = pd.read_csv(acs)
acs2df = pd.read_csv(acs2)

sdf = nrdf.merge(nr2df, on="patient_num", how="outer").merge(acsdf, on="patient_num", how="outer").merge(acs2df, on="patient_num", how="outer")

v = "/var/fhir/FHIR_vector"
e = "/var/fhir/other_processed/env"
e2 = "/var/fhir/other_processed/env5"

years = range(2010, 2016)

def join_env(pdf, env_fn):
    if os.path.isfile(env_fn):
        envdf = pd.read_csv(env_fn)
        penvdf = pdf.merge(envdf, on="start_date", how="left")
    else:
        penvdf = pdf
    return penvdf

def extractYear(x):
    return int(x[0:4])

for f in progressbar(os.listdir(v), redirect_stdout=True):
    print(f"processing {f}")
    p = f[:-4]
    fn = f"{v}/{f}"
    env_fn = f"{e}/{p}"
    env2_fn = f"{e2}/{p}"
    pdf = pd.read_csv(fn)

    penvdf = join_env(pdf, env_fn)
    penv2df = join_env(penvdf, env2_fn)

    if "year" not in penv2df.columns:
        penv2df["year"] = penv2df["start_date"].apply(extractYear)

    padf = penv2df.merge(sdf, on="patient_num", how="left")
    for year2 in years:
        output_dir = f"/var/fhir/icees/{year2}/per_patient"
        os.makedirs(output_dir, exist_ok=True)
        padf[padf["year"] == year2].to_csv(f"{output_dir}/{p}", index=False)
