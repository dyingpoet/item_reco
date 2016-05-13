import numpy as np
import sys
#from scipy.stats.mstats import mquantiles

import math
import functools

def percentile(N, percent, key=lambda x:x):
    """
    Find the percentile of a list of values.

    @parameter N - is a list of values. Note N MUST BE already sorted.
    @parameter percent - a float value from 0.0 to 1.0.
    @parameter key - optional key function to compute value from each element of N.

    @return - the percentile of the values
    """
    if not N:
        return None
    k = (len(N)-1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return d0+d1

# median is 50th percentile.
#median = functools.partial(percentile, percent=0.5)

#print >> sys.stderr, np.__version__

nQtl = int(sys.argv[1])
qtl = [float(1.0*i/nQtl) for i in xrange(1,nQtl+1)]
#qtl = [float(100.0*i/nQtl) for i in xrange(1,nQtl+1)]

lastSc=""
memberList = []
scoreList = []
for line in sys.stdin:
    member, sc, score = line.strip().split('|')
    score = float(score)
    if sc != lastSc:
        #scoreList = np.array(scoreList)
        #scoreQtl = mquantiles(scoreList,prob=qtl)
        #scoreQtl = np.percentile(scoreList,qtl)
        scoreListSort = sorted(scoreList)
        scoreQtl = np.array([percentile(scoreListSort,x) for x in qtl])
        for i in xrange(len(scoreList)):
            idx = (np.argmax( (scoreQtl + 1e-12) >= scoreList[i] ) + 1.0) / nQtl
            print '|'.join([memberList[i],lastSc,str(idx)])
            #print '|'.join([memberList[i],lastSc,str(scoreList[i])])
        memberList = []
        scoreList = []
        lastSc = sc

    memberList.append(member)
    scoreList.append(score)


#scoreList = np.array(scoreList)
#scoreQtl = mquantiles(scoreList,prob=qtl)
#scoreQtl = np.percentile(scoreList,qtl)
scoreListSort = sorted(scoreList)
scoreQtl = np.array([percentile(scoreListSort,x) for x in qtl])
#scoreList[scoreList > scoreCap] = scoreCap
for i in xrange(len(scoreList)):
    idx = (np.argmax( (scoreQtl + 1e-12) >= scoreList[i] ) + 1.0) / nQtl
    print '|'.join([memberList[i],lastSc,str(idx)])
    #print '|'.join([memberList[i],lastSc,str(scoreList[i])])



