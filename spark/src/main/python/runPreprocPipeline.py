import sys
from utils import submit
import subprocess
import os
import tempfile

host_name, config = sys.argv[1:]

tf = tempfile.NamedTemporaryFile(delete=False)
tf.close()

tfname = tf.name

subprocess.call(["dhall-to-yaml", config, tfname])

submit(host_name, "datatrans.PreprocPipeline", "--config=" + tfname)

os.unlink(tfname)

