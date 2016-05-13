import sys

sys.path.append('.')
from bag import *

for line in sys.stdin:
    words = line.strip().split('|')
    membership_nbr, pdcardholder_nbr, membership_create_date, cat_subcat_nbr, pref_bag_str = words
    pref_bag = from_bag(pref_bag_str)
    itemList = [x[0] for x in pref_bag]
    for idx, item in enumerate(itemList): 
        print '|'.join([membership_nbr, pdcardholder_nbr, membership_create_date, cat_subcat_nbr,item,str(idx+1)])


