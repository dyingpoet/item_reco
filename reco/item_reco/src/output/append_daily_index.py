import sys

sys.path.append('.')


dailyIndexFile = 'dailyIndex.txt'

weekday = {}

with open(dailyIndexFile,'r') as inputObj:
    for line in inputObj:
        words = line.strip().split('\t')
        item, day, index = words
        idx = (int(day) - 2) % 7
        score = float(index) / 7.0
        if item in weekday:
            weekday[item][idx] = score 
        else:
            weekday[item] = [0.0] * 7 
            weekday[item][idx] = score 

for item in weekday:
    weekRatio = [str(x) for x in weekday[item]]
    #weekday[item] = '{('+','.join(weekRatio)+')}'
    weekday[item] = '('+','.join(weekRatio)+')'
    
weekRatio = [str(1.0/7.0)] * 7
#weekdayDefault = '{('+','.join(weekRatio)+')}'
weekdayDefault = '('+','.join(weekRatio)+')'


for line in sys.stdin:
    words = line.strip().split('|')
    item = words[6]
    #print '|'.join([])
    if item in weekday:
        print line.rstrip()+'|'+ weekday[item]
    else:
        print line.rstrip()+'|'+ weekdayDefault

