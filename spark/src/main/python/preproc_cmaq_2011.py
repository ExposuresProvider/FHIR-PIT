import glob
import os.path
import shutil
import sys

input_dir = sys.argv[1]
output_dir = sys.argv[2]

dirs = sorted(glob.glob(os.path.join(input_dir, "C*")))
for i, dirname in enumerate(dirs):
    print("processing", i, "/", len(dirs), dirname)
    for filename in glob.glob(os.path.join(dirname, "*.csv")):
        shutil.copy(filename, os.path.join(output_dir, os.path.basename(filename).replace("CMAQ_2011_extractions_12k.", "")))

