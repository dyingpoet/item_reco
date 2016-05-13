import sys

sys.path.append('.')
from bag import *

for line in sys.stdin:
    words = line.strip().split('|')
    club, sc, pref_bag_str = words
    pref_bag = from_bag(pref_bag_str)
    itemList = [x[2] for x in pref_bag]
    for idx, item in enumerate(itemList): 
        print '|'.join([club,sc,item,str(idx+1)])


