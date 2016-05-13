#!/usr/bin/env python
# Filename : CoboughtModelUserHist.py

#############################################################
#####                                                 	#####
#####    Co-bought streaming implementation             #####
#####    Author : Jun Li                                #####
#####    Create Date : 08-Aug, 2013                    	#####
#####                                                 	#####
#############################################################

from __future__ import division
import os
import sys
import math
import numpy as np
import CoboughtModelConfig as Config
import CoboughtModelUtil as Util

#############################################################
#####    Class Definition                               #####
#############################################################

class CoboughtModelUserHist(object):
    """
    """
    def __init__(self, user, config) :
        """
        function : Initialization Function for Cobought streaming implementation
        """
        self.user = user 
	self.topN = [5,10,20]
	self.nSubcat = config.nSubcat
        self.outputThreshold = config.outputThreshold
        
        # Initialization for CoboughtModel
        self.trainingItemList = []
        #self.testingItemList = []
        self.trainingItemVal = []
        

    ####   STCoboughtModel  #### 
     
    # Add Values for STCoboughtModel
    def addSTCoboughtModel(self, record, config):
        """
        Add to the Cobought list
        """
        try:
	    self.trainingItemList.append(int(config.coboughtItemAll[record.subcat]))
            self.trainingItemVal.append(float(record.val))
	    #self.trainingItemList.append(int(record.subcat))
        except:
            print >> sys.stderr, record.subcat

    def finalSTCoboughtModel(self, config):
        '''
        calculate the aggregated variables
        '''
	if len(self.trainingItemList) > 0:
            ### anchor exclusion
            subcatExclusion = ["82","84","73","35","81","87"]
            scItemExclusion = [int(config.coboughtItemAll[i]) for i in config.coboughtItemAllList if i[0:2] in subcatExclusion]
            anchorExclusion = [int(config.coboughtItemAll[i]) for i in ["5612","3898","4811","9420","4030","5832","0490","0132","0470","9410","0445","5630","0460","0467"]]
            anchorExclusion.extend(scItemExclusion)
            #anchorExclusion = [i for i in anchorExclusion if i < self.nSubcat]
            config.coboughtData[anchorExclusion,] = 0 

            ### dog-cat removal, only keeps 1. dog: 0810, 0812, 0850. 2. cat: 0815, 0822, 0855, 0820
            animalAnchorExclusion = [int(config.coboughtItemAll[i]) for i in config.coboughtItemAllList if i[0:2]=="08" and i not in ["0810","0812", "0850", "0815", "0822", "0855", "0820"]] 
            animalRecoExclusion = [int(config.coboughtItem[i]) for i in config.coboughtItemList if i[0:2]=="08" and i not in ["0810","0812", "0850", "0815", "0822", "0855", "0820"]] 
            config.coboughtData[animalAnchorExclusion,] = 0 
            config.coboughtData[:,animalRecoExclusion] = 0 
            dogAnchorExclusion = [int(config.coboughtItemAll[i]) for i in ["0810","0812", "0850"]]
            dogRecoExclusion = [int(config.coboughtItem[i]) for i in ["0810","0812", "0850"] if i in config.coboughtItem]
            catAnchorExclusion = [int(config.coboughtItemAll[i]) for i in ["0815", "0822", "0855", "0820"]]
            catRecoExclusion = [int(config.coboughtItem[i]) for i in ["0815", "0822", "0855", "0820"] if i in config.coboughtItem]
            config.coboughtData[catAnchorExclusion,:][:,dogRecoExclusion] = 0 
            config.coboughtData[dogAnchorExclusion,:][:,catRecoExclusion] = 0 

            #weight = 0.5*(np.array(self.trainingItemVal)+1.0)
            #coboughtScore = np.dot(weight, config.coboughtData[self.trainingItemList,]) / weight.sum()
            #try:
            #    coboughtScore = np.dot(weight, config.coboughtData[self.trainingItemList,]) / weight.sum()
            #except:
            #    print >> sys.stderr, "user:", self.user
            #    print >> sys.stderr, "itemList:", self.trainingItemList
            #    print >> sys.stderr, "weight:", weight
            #    print >> sys.stderr, "len(weight):", len(weight)
            #    print >> sys.stderr, "score:", config.coboughtData[self.trainingItemList,]
            #    print >> sys.stderr, "score.shape():", np.shape(config.coboughtData[self.trainingItemList,])
            #    raise 

            ### old version
            #coboughtScore = np.dot(np.array(self.trainingItemVal), config.coboughtData[self.trainingItemList,]) / sum(np.array(self.trainingItemVal))

            #coboughtScore = np.dot(np.array(self.trainingItemVal)*config.idfData[self.trainingItemList], config.coboughtData[self.trainingItemList,]) / sum(np.array(self.trainingItemVal)*config.idfData[self.trainingItemList])

	    #coboughtScore = np.max(config.coboughtData[self.trainingItemList,],0)
	    #coboughtArgmax = np.argmax(config.coboughtData[self.trainingItemList,],0)

	    #coboughtScore = np.max(np.multiply(self.trainingItemVal,config.coboughtData[self.trainingItemList,].T).T,0)
	    #coboughtArgmax = np.argmax(np.multiply(self.trainingItemVal,config.coboughtData[self.trainingItemList,].T).T,0)

	    #coboughtScore = np.max(np.multiply(np.array(self.trainingItemVal)*config.idfData[self.trainingItemList],config.coboughtData[self.trainingItemList,].T).T,0)
	    #coboughtArgmax = np.argmax(np.multiply(np.array(self.trainingItemVal)*config.idfData[self.trainingItemList],config.coboughtData[self.trainingItemList,].T).T,0)

            #coboughtScore = np.multiply(coboughtScore, config.seasonalityScore[config.numMonth]) 


            ### version orig + option control
            if config.method == "Avg":
                coboughtScore = np.dot(np.array(self.trainingItemVal), config.coboughtData[self.trainingItemList,]) / sum(np.array(self.trainingItemVal))

            if config.method == "AvgIdf":
                coboughtScore = np.dot(np.array(self.trainingItemVal)*config.idfData[self.trainingItemList], config.coboughtData[self.trainingItemList,]) / sum(np.array(self.trainingItemVal)*config.idfData[self.trainingItemList])

            if config.method in ("Avg","AvgIdf") and config.targetIdf == 'Y':
                    coboughtScore = np.multiply(coboughtScore, config.idfData) 
           
            if config.method == "MaxCobought":
                if config.targetIdf == 'Y':
	            coboughtScore = np.max(np.multiply(config.coboughtData[self.trainingItemList,],config.idfData),0)
	            coboughtArgmax = np.argmax(np.multiply(config.coboughtData[self.trainingItemList,],config.idfData),0)
                else:
	            coboughtScore = np.max(config.coboughtData[self.trainingItemList,],0)
	            coboughtArgmax = np.argmax(config.coboughtData[self.trainingItemList,],0)

            if config.method == "MaxCoboughtVisit":
                if config.targetIdf == 'Y':
	            coboughtScore = np.max(np.multiply(np.multiply(self.trainingItemVal,config.coboughtData[self.trainingItemList,].T).T,config.idfData),0)
	            coboughtArgmax = np.argmax(np.multiply(np.multiply(self.trainingItemVal,config.coboughtData[self.trainingItemList,].T).T,config.idfData),0)
                else:
	            coboughtScore = np.max(np.multiply(self.trainingItemVal,config.coboughtData[self.trainingItemList,].T).T,0)
	            coboughtArgmax = np.argmax(np.multiply(self.trainingItemVal,config.coboughtData[self.trainingItemList,].T).T,0)

            if config.method == "MaxCoboughtVisitIdf":
                if config.targetIdf == 'Y':
	            coboughtScore = np.max(np.multiply(np.multiply(np.array(self.trainingItemVal)*config.idfData[self.trainingItemList],config.coboughtData[self.trainingItemList,].T).T,config.idfData),0)
	            coboughtArgmax = np.argmax(np.multiply(np.multiply(np.array(self.trainingItemVal)*config.idfData[self.trainingItemList],config.coboughtData[self.trainingItemList,].T).T,config.idfData),0)
                else:
	            coboughtScore = np.max(np.multiply(np.array(self.trainingItemVal)*config.idfData[self.trainingItemList],config.coboughtData[self.trainingItemList,].T).T,0)
	            coboughtArgmax = np.argmax(np.multiply(np.array(self.trainingItemVal)*config.idfData[self.trainingItemList],config.coboughtData[self.trainingItemList,].T).T,0)


            ### multiply by seasonality
            coboughtScore = np.multiply(coboughtScore, config.seasonalityScore[config.numMonth]) 


            ### reco exclusion
            #subcatExclusion = ["82","84"]
            scItemExclusion = [int(config.coboughtItemAll[i]) for i in config.coboughtItemAllList if i[0:2] in subcatExclusion]
            #recoExclusion = [int(config.coboughtItemAll[i]) for i in ["5612","3898","4811","9420","4030","5832","0490","0132","0470","0445","0460","0467"]]
            #recoExclusion.extend(scItemExclusion)
            recoExclusion = scItemExclusion
            recoExclusion = [i for i in recoExclusion if i < self.nSubcat]
            coboughtScore[recoExclusion] = 0

            ### reward exclusion [recommend] 
            trainingExclusion = [i for i in self.trainingItemList if i < self.nSubcat]
	    coboughtScore[trainingExclusion] = -1 

            ### reward inclusion [reward]
            #trainingComp = [i for i in xrange(self.nSubcat) if i not in self.trainingItemList]
	    #coboughtScore[trainingComp] = -1 



	    #rankedList = np.argsort(coboughtScore)
	    for i in xrange(len(coboughtScore)):
                if coboughtScore[i] > config.outputThreshold:
                #if coboughtScore[i] > 1e-12:
                #if coboughtScore[i] > -0.5:
                    if config.method in ("Avg","AvgIdf"):
                        print('|'.join([str(self.user),str(config.coboughtItemList[i]),str(coboughtScore[i])]))
                    else:
                        print('|'.join([str(self.user),str(config.coboughtItemList[i]),str(coboughtScore[i]),config.coboughtItemAllList[self.trainingItemList[coboughtArgmax[i]]]]))
            
                    
    # Generate Output for STCoboughtModel
    def genSTCoboughtModel(self) :
        '''
        print results
        '''
        pass
