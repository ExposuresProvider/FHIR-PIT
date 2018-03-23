import sys
import pandas as pd

input_file = sys.argv[1]
output_file = sys.argv[2]

df = pd.read_csv(input_file)
df = df[["CONCEPT_PATH","CONCEPT_CD"]][df.CONCEPT_CD.str.startswith("MDCTN:")]

def getRxnorm(concept_path):
    for s in concept_path.split("/"):
        if s.startswith("RX:"):
            return s
    return None

df["RXNORM"] = df["CONCEPT_PATH"].map(getRxnorm)
df = df[["CONCEPT_CD", "RXNORM"]]

df.to_csv(output_file, index=False)

