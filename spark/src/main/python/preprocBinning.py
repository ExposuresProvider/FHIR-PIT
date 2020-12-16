import os                               
import sys                              
import json                             
from preprocPatient import *            
from preprocVisit import *

year_start, year_end, config_file, input_dir, output_dir = sys.argv[1:]

for year in range(int(year_start), int(year_end) + 1):
    print(year)
    input_file_p = f"{input_dir}/{year}/all_patient"
    output_file_p = f"{output_dir}/{year}patient"
    preproc_patient(config_file, input_file_p, output_file_p)
    input_file_v = f"{input_dir}/{year}/all_visit"
    output_file_v = f"{output_dir}/{year}visit"
    preproc_visit(config_file, input_file_v, output_file_v)
