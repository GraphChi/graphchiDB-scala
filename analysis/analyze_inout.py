'''
Analyze inout file
'''

import os
import sys


f = sys.argv[-1]

data = open(f).read()

values = [[float(x) for x in ln.split(",")] for ln in data.split("\n")[1:] if len(ln) > 1]

outchecksum = sum([d[0] for d in values])
totalouttime = sum([d[1] for d in values])


inchecksum = sum([d[2] for d in values])
totalintime = sum([d[3] for d in values])

print f
print "  OUT: time=%f secs,  %f per query, %f queries per sec, checksum=%f" % (totalouttime * 0.000001, totalouttime * 0.000001 / len(values), len(values ) / (totalouttime * 0.000001), outchecksum)
print "  IN: time=%f secs, checksum=%f" % (totalintime * 0.000001, inchecksum)