import os
import sys
from utils import submit

input_dir = sys.argv[1]
time_series = sys.argv[2]
patient_dimension = sys.argv[3]
mapp = sys.argv[4]
output_dir = sys.argv[5]
cache_dir = sys.argv[6]
host_name = sys.argv[7]


submit(host_name, cache_dir, "datatrans.PreprocPerPatSeriesToVector",
           "--patient_dimension={0}".format(patient_dimension),
           "--input_directory=" + input_dir + "/",
           "--time_series=" + time_series,
           "--output_prefix=" + output_dir + "/",
           "--regex_observation=" + """ICD9:(799[.]02|780[.]60)|ICD10:(R09[.]02|R50[.]9)|""" + """ICD9:((493|464|496|786|481|482|483|484|485|486)[.].*)|ICD10:((J45|J05|J44|J66|R05|J12|J13|J14|J15|J16|J17|J18)[.].*)|""" + """ICD9:278.00|ICD10:E66[.]([^3].*|3.+)|""" + """LOINC:(33536-4|13834-7|26449-9|711-2|712-0|26450-7|713-8|714-6|26499-4|751-8|753-4|26511-6|770-8|23761-0|1988-5|30522-7|11039-5|35648-5|76485-2|76486-0|14634-0|71426-1)""",
           "--regex_visit=" + """INPATIENT|OUTPATIENT|EMERGENCY""",
           "--regex_observation_filter_visit=" + """ICD9:((493|464|496|786|481|482|483|484|485|486)[.].*)|ICD10:((J45|J05|J44|J66|R05|J12|J13|J14|J15|J16|J17|J18)[.].*)""",
           "--map=" + mapp, *sys.argv[8:])

