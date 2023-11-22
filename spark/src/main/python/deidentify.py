import os
import sys
from deidentifyPatient import deidentify_patient
from deidentifyVisit import deidentify_visit
from preprocUtils import get_all_index_patient_across_years, get_patient_file

config_file, input_dir, output_dir, *study_periods = sys.argv[1:]

os.makedirs(output_dir, exist_ok=True)

# get patient to index mapping across study periods
all_index_patient_file_p = get_all_index_patient_across_years(input_dir, output_dir, study_periods)

for year in study_periods:
    input_file_p = get_patient_file(input_dir, year)
    output_file_p = f"{output_dir}/{year}patient"
    deidentify_patient(input_file_p, output_file_p, all_index_patient_file_p)
    input_file_v = get_patient_file(input_dir, year, visit=True)
    output_file_v = f"{output_dir}/{year}visit"
    deidentify_visit(input_file_v, output_file_v)
