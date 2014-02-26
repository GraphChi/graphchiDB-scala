'''
Analyze inout file
'''

import os
import sys
import numpy as np


table = {}
for f in sys.argv[1:]:
    data = open(f).read()

    if data.find(",") > 0:
        delim = ","
    else:
        delim = "\t"

    values = [[float(x) for x in ln.split(delim)] for ln in data.split("\n")[1:] if len(ln) > 1]

    fofchecksum = sum([d[0] for d in values])
    totalfoftime = sum([d[1] for d in values])

    # note msecs
    times =  [0.001 * d[1] for d in values]
    sdev = np.std(times)

    if "pin_false_sparseindex_false" in f:
        group = "1no-index"
    elif "pin_true_sparseindex_true" in f:
        group = "3elias-gamma"
    elif "pin_false_sparseindex_true" in f:
        group  = "2sparse-index"
    else:
        assert(False)

    if "in_tw" in f:
        group = "in-edges:" + group
    else:
        group = "out-edges:" + group


    if group in table:
        table[group] = table[group] + times
    else:
        table[group] = times


    print f
    print "  FOF: time=%f secs,  %f per query, sdev=%f  , %f queries per sec, checksum=%f, max=%f"  %  \
            (totalfoftime * 0.000001, np.mean(times), sdev, len(values ) / (totalfoftime * 0.000001), fofchecksum, 0.000001 * max([d[1] for d in values]))


print "test,mean,sdev,count"
for group in sorted(table.keys()):
    t = np.array(table[group])
    group = group.replace("1", "").replace("2", "").replace("3", "")  #dirty
    print "%s,%f,%f,%d" % (group, np.mean(t), np.std(t), len(t))




