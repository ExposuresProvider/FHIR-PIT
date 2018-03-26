import os
import sys
from utils import submit

dir = sys.argv[3]
cache_dir = sys.argv[4]
host_name = sys.argv[5]

def process_pids(pids):
    pids0 = ",".join(pids)
    submit(host_name, cache_dir, "datatrans.PreprocPerPatSeriesToVector",
           "--patient_num_list={0}".format(pids0),
           "--input_directory=" + dir + "/json",
           "--output_prefix=" + dir + "/json/vector",
           "--regex=" + """ICD9:((493|464|496|786|481|482|483|484|485|486)[.].*|278.00)|ICD10:((J45|J05|J44|J66|R05|J12|J13|J14|J15|J16|J17|J18)[.].*|E66[.]([^3].*|3.+))|"""
                        """LOINC:(33536-4|13834-7|26449-9|711-2|712-0|26450-7|713-8|714-6|26499-4|751-8|753-4|26511-6|770-8|23761-0|1988-5|30522-7|11039-5|35648-5|76485-2|76486-0|14634-0|71426-1)""",
           "--map=" + sys.argv[6],
           log = dir + "/json/stdoutvec" + pids0,
           log2 = dir + "/json/stderrvec" + pids0)

with open(sys.argv[1]) as f:
    count = int(sys.argv[2])
    pids = []
    n = 0
    for line in f.readlines():
        n += 1
        pid = line.rstrip("\n")
        print("processing", pid)
        if os.path.exists(dir + "/json/vector"+pid):
            print(pid + " exists")
        else:
            pids.append(pid)
            if len(pids) == count:
                process_pids(pids)
                pids.clear()
        print("processed", n)

    if len(pids) != 0:
        process_pids(pids)
