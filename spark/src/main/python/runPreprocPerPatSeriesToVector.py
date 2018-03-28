import os
import sys
from utils import submit

dir = sys.argv[1]
cache_dir = sys.argv[2]
host_name = sys.argv[3]

submit(host_name, cache_dir, "datatrans.PreprocPerPatSeriesToVector",
           "--patient_dimension={0}/patient_dimension.csv".format(dir),
           "--input_directory=" + dir + "/json",
           "--environmental_data=" + dir,
           "--output_prefix=" + dir + "/vector/",
           "--regex=" + """ICD9:((493|464|496|786|481|482|483|484|485|486)[.].*|278.00)|ICD10:((J45|J05|J44|J66|R05|J12|J13|J14|J15|J16|J17|J18)[.].*|E66[.]([^3].*|3.+))|"""
                        """LOINC:(33536-4|13834-7|26449-9|711-2|712-0|26450-7|713-8|714-6|26499-4|751-8|753-4|26511-6|770-8|23761-0|1988-5|30522-7|11039-5|35648-5|76485-2|76486-0|14634-0|71426-1)""",
           "--map=" + sys.argv[4],
           "--start_date=2010-01-01",
           "--end_date=2012-01-01")

