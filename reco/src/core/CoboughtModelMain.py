#!/usr/bin/env python
# Filename : CoboughtModelMain.py

#############################################################
#####                                                 	#####
#####    Main Function For Co-bought Streaming Design   #####
#####    Author : Jun Li                                #####
#####    Create Date : 07-Aug, 2013                     #####
#####                                                 	#####
#############################################################

#############################################################
##### Step 1. cache the cobought pairwise probabilty in pig, python read the cache
#####
##### Step 2. for each user, output the top N (N=10) recommended items <user,item_1>, <user,item_2>, ... in python
#####
##### Step 3. left join the test data with the ouput items in pig
#############################################################

from __future__ import division
import os
import sys
from math import *

sys.path.append('.')

import CoboughtModelConfig as Config
import CoboughtModelBackendCalcEngine as CB

#############################################################
#####    Global Variables                               #####
#############################################################

#############################################################
#####    Main Function                                  #####
#############################################################

def main(argv) :
    instCon = Config.CoboughtModelConfig(argv)
    instCB = CB.CoboughtModelBackendCalcEngine()
    instCB.process(instCon)

#############################################################
#####    Class Definition                               #####
#############################################################

#############################################################
#####    Main Function Entrance                         #####
#############################################################

if __name__ == "__main__" :
    main(sys.argv)

