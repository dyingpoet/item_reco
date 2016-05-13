#!/usr/bin/env python
# Filename : CoboughtModelInput.py

#############################################################
#####                                                 	#####
#####    Input Definition                               #####
#####    Author : Jun Li                                #####
#####    Create Date : 08-Aug, 20123                    #####
#####                                                 	#####
#############################################################
import os
import sys

#############################################################
#####    Global Variables                               #####
#############################################################


#############################################################
#####    Class Definition                               #####
#############################################################

class CoboughtModelInput :
    def __init__(self, line, dlm) :
        
        ''' Parse and store streaming input into one instance 
        '''
        words = line.strip().split(dlm)
        self.user                    =     words[0]
        self.subcat                  =     words[1]
        self.val                     =     words[2]
        

