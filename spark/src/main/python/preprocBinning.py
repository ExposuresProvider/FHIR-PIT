import sys
import os
from preprocPatient import preproc_patient
from preprocVisit import preproc_visit

config_file, input_dir, output_dir, *study_periods = sys.argv[1:]

os.makedirs(output_dir, exist_ok=True)


for year in study_periods:
    print(year)
    input_file_p = f"{input_dir}/{year}/all_patient"
    output_file_p = f"{output_dir}/{year}patient"
    preproc_patient(config_file, input_file_p, output_file_p)
    input_file_v = f"{input_dir}/{year}/all_visit"
    output_file_v = f"{output_dir}/{year}visit"
    preproc_visit(config_file, input_file_v, output_file_v)
