# this is an example of how to use the sparse table extraction tools
# use python 2.7 for load_df

from datetime import datetime
from datatrans.sparse.import_df import load_df 

def cb(r):
        birth_date = datetime.strptime(r['birth_date'], "%Y-%m-%d %H:%M:%S")
        curr_date = datetime.now()
        age = curr_date.year - birth_date.year - ((curr_date.month, curr_date.day) < (birth_date.month, birth_date.day))
        print(".")
        if age <= 50:
            print(r)

load_df("endotype-wide.csv", cb)
