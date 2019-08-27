import sys
from utils import submit

host_name, config = sys.argv[1:]
submit(host_name, "datatrans.PreprocPerPatSeries", "--config=" + config)


