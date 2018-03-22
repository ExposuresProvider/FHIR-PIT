import glob
import os.path
import shutil
import sys

input_dir = sys.argv[1]
output_dir = sys.argv[2]

for filename in glob.glob(os.path.join(input_dir, "C*", "*.csv")):
    print("processing", filename)
    shutil.move(filename, os.path.join(output_dir, os.path.basename(filename).replace("_extractions_12k", "")))

