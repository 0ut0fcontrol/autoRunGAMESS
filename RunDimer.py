#!/usr/bin/python
print "1. Call generate_coords() and call gamess for all configurations."

import os
import sys

tempfile = os.popen('ls *.inp','r')
inps = tempfile.readlines()
tempfile.close()

for inp in inps:
    #temppipe = os.popen('/pubhome/lhua/QMScoring/scripts/rungms_py -f %s -o %s.log' % (inp.strip(),inp.strip()),'r')
    temppipe = os.popen('rungms_py -f %s -o %s.log' % (inp.strip(),inp.strip()),'r')
    temppipe.read()
    temppipe.close()

sys.exit()
