import os                               
import sys                              
import json                             
from preprocPatient import *            
from preprocVisit import *

# input_dir = "/var/fhir/icees2"        
# output_dir = "/var/fhir/icees2/output"
                                        
input_dir, output_dir = sys.argv[1:]    

for year in range(2010, 2020):          
    print(year)
    input_file_p = f"{input_dir}/{year}/all_patient"
    output_file_p = f"{output_dir}/{year}patient"
    preproc_patient(input_file_p, output_file_p)
    input_file_v = f"{input_dir}/{year}/all_visit"
    output_file_v = f"{output_dir}/{year}visit"
    preproc_visit(input_file_v, output_file_v)
