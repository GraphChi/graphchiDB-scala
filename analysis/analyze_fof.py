'''
Analyze inout file
'''

import os
import sys


f = sys.argv[-1]

data = open(f).read()

if data.find(",") > 0:
    delim = ","
else:
    delim = "\t"

values = [[float(x) for x in ln.split(delim)] for ln in data.split("\n")[1:] if len(ln) > 1]

fofchecksum = sum([d[0] for d in values])
totalfoftime = sum([d[1] for d in values])



print f
print "  FOF: time=%f secs,  %f per query, %f queries per sec, checksum=%f" % (totalfoftime * 0.000001, totalfoftime * 0.000001 / len(values), len(values ) / (totalfoftime * 0.000001), fofchecksum)
