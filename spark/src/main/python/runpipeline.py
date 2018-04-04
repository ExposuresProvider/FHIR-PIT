from os import system

import sys

dir = sys.argv[1]
cache_dir = sys.argv[2]

# check per pat
system("python runChunkPerPat.py " + dir + " " + cache_dir)

# headers
system("python runheader.py " + dir + " " + cache_dir)

# pat to json
system("python run.py " + dir + "/json/patient_num 16" + dir + " " + cache_dir)

# json to vector
system("python runvec.py " + dir + "/json/patient_num 16" + dir + " " + cache_dir)
