import sys
import numpy as np

scMap = {}
idx = 0

catsubFile=sys.argv[1]
#catsubFile='rewati_seasonality.raw.catsub'
#for line in open('rewati_seasonality.raw.catsub','r'):
for line in open(catsubFile,'r'):
    scMap[line.strip()] = idx
    idx += 1
#numSC = int(sys.argv[1])

numSC = len(scMap)
seasonality = np.zeros((numSC,12))

for line in sys.stdin:
    #words = line.strip().split()
    #print words[2].split('-')[1]
    catsub, score, dt = line.strip().split()
    mon = int(dt.split('-')[1])
    score = float(score)
    seasonality[scMap[catsub],mon-1] = score

for sc in scMap:
    print sc + '|' + '|'.join([str(f) for f in seasonality[scMap[sc]]])
