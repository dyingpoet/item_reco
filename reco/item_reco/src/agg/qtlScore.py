import numpy as np
import sys
#from scipy.stats.mstats import mquantiles


print >> sys.stderr, np.__version__

nQtl = int(sys.argv[1])
#qtl = [float(1.0*i/nQtl) for i in xrange(1,nQtl+1)]
qtl = [float(100.0*i/nQtl) for i in xrange(1,nQtl+1)]

lastSc=""
memberList = []
scoreList = []
for line in sys.stdin:
    member, sc, score = line.strip().split('|')
    score = float(score)
    if sc != lastSc:
        scoreList = np.array(scoreList)
        #scoreQtl = mquantiles(scoreList,prob=qtl)
        scoreQtl = np.percentile(scoreList,qtl)
        for i in xrange(len(scoreList)):
            idx = (np.argmax( (scoreQtl + 1e-12) >= scoreList[i] ) + 1.0) / nQtl
            print '|'.join([memberList[i],lastSc,str(idx)])
            #print '|'.join([memberList[i],lastSc,str(scoreList[i])])
        memberList = []
        scoreList = []
        lastSc = sc

    memberList.append(member)
    scoreList.append(score)


scoreList = np.array(scoreList)
#scoreQtl = mquantiles(scoreList,prob=qtl)
scoreQtl = np.percentile(scoreList,qtl)
#scoreList[scoreList > scoreCap] = scoreCap
for i in xrange(len(scoreList)):
    idx = (np.argmax( (scoreQtl + 1e-12) >= scoreList[i] ) + 1.0) / nQtl
    print '|'.join([memberList[i],lastSc,str(idx)])
    #print '|'.join([memberList[i],lastSc,str(scoreList[i])])

