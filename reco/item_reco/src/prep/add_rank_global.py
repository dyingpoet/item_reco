import sys

sys.path.append('.')
from bag import *

for line in sys.stdin:
    words = line.strip().split('|')
    sc, pref_bag_str = words
    pref_bag = from_bag(pref_bag_str)
    itemList = [x[1] for x in pref_bag]
    for idx, item in enumerate(itemList): 
        print '|'.join([sc,item,str(idx+1)])


