#!/usr/bin/env python
# Filename : CoboughtModelBackendCalcEngine.py

################################################################
#####                                                 	   #####
#####    Cobought Backend Calculation Engine Definition    #####
#####    Author : Jun Li                                   #####
#####    Create Date : 08-Aug, 2013                        #####
#####                                                 	   #####
################################################################

from __future__ import division
import os
import sys
import numpy as np
import CoboughtModelUtil as Util
import CoboughtModelConfig as Config
import CoboughtModelUserHist as UserHist
from CoboughtModelInput import CoboughtModelInput

#############################################################
#####    Class Definition                               #####
#############################################################

class CoboughtModelBackendCalcEngine(object) :
    def __init__(self) :
        pass

    def process(self, config) :
        previousKey = ''
        
	# cobought backend calc engine takes the joined file of training and testing
	# <user, subcat, tag> where tag = 0:'training' or 1:'testing', the score / prob is not used here as the input 
        #for line in open('dummy','r'):
        for line in sys.stdin:
            input = CoboughtModelInput(line,Util.DLM)
            
            #if input.subcat in config.coboughtItem :
	    if 1: 
                currentKey = str(input.user)
                
                if currentKey != previousKey:
                    
                    if previousKey != '':
                        userHist.finalSTCoboughtModel(config)
                        userHist.genSTCoboughtModel()
                        
                    previousKey = currentKey
                    userHist = UserHist.CoboughtModelUserHist(input.user, config)
                
                userHist.addSTCoboughtModel(input,config) 
                #userHist.addSTCoboughtModel(input) 

            else:
                continue
        
        # output the results for the final Key
        userHist.finalSTCoboughtModel(config)
        userHist.genSTCoboughtModel()


