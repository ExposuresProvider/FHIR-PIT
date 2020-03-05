from preprocPatient import *
from preprocVisit import *
import os

input_dir = "/var/fhir/icees2"
output_dir = "/var/fhir/icees2/output"

for year in range(2010, 2017):
    print(year)
    input_file_p = f"{input_dir}/{year}/all_patient_cor"
    output_file_p = f"{output_dir}/{year}patient"
    preproc_patient(input_file_p, output_file_p)
    input_file_v = f"{input_dir}/{year}/all_visit_cor"
    output_file_v = f"{output_dir}/{year}visit"
    preproc_visit(input_file_v, output_file_v)
