#!/apps/base/python3/bin/python3

import sys
import os
import shutil

import re
dqr_regex = re.compile("D\d{6}(\.)*(\d)*")
datastream_regex = re.compile("(acx|awr|dmf|fkb|gec|hfe|mag|mar|mlo|nic|nsa|osc|pgh|pye|sbs|shb|tmp|wbu|zrh|asi|cjc|ena|gan|grw|isp|mao|mcq|nac|nim|oli|osi|pvc|rld|sgp|smt|twp|yeu)\w+\.(\w){2}")

try:
	if sys.argv[1] == "-h" or sys.argv[1] == "--help":
        print("arg 1 = dqr\narg 2 = datastream")
        exit()
except IndexError:
	print("Running ncreview.")
	pass

cwd = os.getcwd()
dqr = dqr_regex.search(cwd).group()
datastream = datastream_regex.search(cwd).group()
site = datastream[:3]

post_proc = os.environ['POST_PROC']
log = "{}/{}/ncr_{}.log".format(post_proc, dqr, datastream)
out_dir = "{}/{}".format(post_proc, dqr)

if not os.path.isdir(out_dir):
	print("Making output directory @ {}".format(out_dir))
	os.makedirs(out_dir)
if os.path.isfile(log):
	print("Removing old log file - {}".format(log))
    os.remove(log)

cmd = "ncreview /data/archive/{}/{}/ {} -n ncr_{} -w {} > {}".format(site, datastream, os.environ['PWD'], datastream, out_dir , log)
print("cmd = {}".format(cmd))

os.system(cmd)
os.system(f"cat {log}")