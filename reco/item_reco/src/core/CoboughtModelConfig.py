#!/usr/bin/env python
# Filename : CoboughtModelConfig.py

#############################################################
#####                                                 	#####
#####    Configuration File to read in config           #####
#####    Author : Jun Li                                #####
#####    Create Date : 08-Aug, 2013                     #####
#####                                                 	#####
#############################################################

import os
import sys
import copy
import numpy as np
import CoboughtModelUtil as Util

def readFile(file) :
    try :
        return open(file, 'r').readlines()
    except IOError, e :
        print >> sys.stderr, "cannot open file : " + str(e) + file


#############################################################
#####    Class Definition                               #####
#############################################################

class CoboughtModelConfig(object) :
    def __init__(self, argv) :
	# Ideally, the cobought data is preprocessed where for each item1 group, all the item2 terms are desc sorted by the prob
        self.numMonth = int(argv[5])
        self.nSubcat = int(argv[6])  #548
        self.nSubcatAll = 4527
        self.method = argv[7]
        self.targetIdf = argv[8]
        self.coboughtItem = {}
        self.coboughtItemList = []
        self.coboughtItemAll = {}
        self.coboughtItemAllList = []
        self.coboughtData = np.zeros((self.nSubcatAll,self.nSubcat))
        self.idfData = np.zeros((self.nSubcat))
        #self.seasonalityData = {}
        self.seasonalityScore = {}
        subcatIndexFile = argv[1]
        coboughtFile = argv[2]
        seasonalityFile = argv[3]
        idfFile = argv[4]
        self.readCacheSubcat(subcatIndexFile)
        self.readCacheCobought(coboughtFile)
        self.readCacheSeasonality(seasonalityFile)
        self.readCacheIdf(idfFile)
       
    def readCacheSubcat(self, subcatIndexFile) :
	idx = 0
        for line in readFile(subcatIndexFile) :
	    self.coboughtItem[line.strip()] = idx
	    self.coboughtItemList.append(line.strip())
	    idx += 1
        self.coboughtItemAll = copy.deepcopy(self.coboughtItem)
        self.coboughtItemAllList = copy.deepcopy(self.coboughtItemList)

    def readCacheCobought(self, coboughtFile) :
        idx = self.nSubcat
        business = ["0100","0101","0102","0103","0104","0105","0106","0107","0108","0109","0110","0111","0112","0113","0114","0115","0118","0119","0120","0121","0122","0123","0124","0125","0128","0130","0131","0132","0133","0134","0135","0136","0138","0139","0140","0141","0144","0150","0152","0153","0154","0155","0156","0157","0158","0159","0160","0161","0163","0165","0169","0170","0171","0172","0173","0174","0175","0176","0180","0181","0182","0183","0185","0186","0187","0189","0190","0191","0192","0197","0198","0199","0297","0311","0323","0397","0400","0404","0405","0406","0407","0408","0409","0411","0415","0416","0422","0430","0445","0451","0470","0473","0480","0489","0492","0497","0597","0697","0797","0897","0997","1097","1197","1297","1397","1497","1597","1697","1797","1897","1997","2097","2197","2397","2497","2526","2597","2600","2601","2602","2603","2604","2605","2606","2607","2608","2609","2610","2611","2612","2613","2614","2615","2616","2620","2621","2622","2623","2624","2625","2626","2634","2640","2641","2642","2643","2644","2645","2650","2651","2652","2653","2654","2655","2656","2657","2658","2659","2660","2661","2662","2663","2664","2665","2670","2671","2672","2673","2674","2675","2680","2681","2682","2683","2684","2685","2686","2687","2688","2690","2692","2695","2697","2901","2925","2997","3141","3197","3297","3397","3497","3625","3690","3697","3701","3702","3707","3713","3729","3733","3738","3746","3748","3768","3789","4097","4197","4306","4308","4309","4311","4325","4331","4341","4355","4360","4367","4369","4371","4381","4385","4387","4390","4393","4396","4399","4402","4404","4411","4414","4471","4476","4497","4604","4606","4609","4611","4613","4617","4637","4639","4641","4644","4651","4654","4655","4660","4662","4670","4671","4675","4697","4797","4902","4905","4906","4907","4908","4909","4910","4912","4919","4920","4922","4926","4931","4949","4951","4960","4964","4965","4972","4979","4987","4989","4990","4993","4997","5197","5297","5397","5427","5497","5697","5797","6097","6197","6497","6504","6536","6697","6797","6897","6997","7097","7297","7497","7697","7801","7997","8201","8204","8236","8237","8243","8249","8296","8697","8897","9097","9368","9497","9597","9600","9602","9603","9612","9618","9620","9623","9626","9627","9628","9638","9639","9644","9650","9651","9652","9654","9689","9997"]
        businessMap = {}
        for i in business:
            businessMap[i] = 1
        for line in readFile(coboughtFile) :
            words = line.strip().split(Util.DLM)
            if len(words) != 3 :
                print >> sys.stderr, "Invalid Field Number in the cobought File, should be 3 fields "
                print >> sys.stderr, line
                continue
            if words[0] not in self.coboughtItemAll:
	        self.coboughtItemAll[words[0]] = idx
	        self.coboughtItemAllList.append(words[0])
	        idx += 1
            if words[1] not in self.coboughtItemAll:
	        self.coboughtItemAll[words[1]] = idx
	        self.coboughtItemAllList.append(words[1])
	        idx += 1
            if words[1] in self.coboughtItem:
                #self.coboughtData[self.coboughtItemAll[words[0]],self.coboughtItem[words[1]]] = float(words[2])
                if words[1] in businessMap:
                    self.coboughtData[self.coboughtItemAll[words[0]],self.coboughtItem[words[1]]] = float(words[2]) * 0.5
                else:
                    self.coboughtData[self.coboughtItemAll[words[0]],self.coboughtItem[words[1]]] = float(words[2])

    def readCacheSeasonality(self, seasonalityFile):
        for i in range(1,13):
            self.seasonalityScore[i] = np.ones(self.nSubcat)
        for line in readFile(seasonalityFile):
            words = line.strip().split(Util.DLM)
            #self.seasonalityData[word[0]] = [int(words[i]) for i in range(1,13)]
            if words[0] in self.coboughtItemList:
                for i in range(1,13):
                    self.seasonalityScore[i][self.coboughtItem[words[0]]] = float(words[i])

    def readCacheIdf(self, idfFile) :
        for line in readFile(idfFile) :
            sc, idfValue = line.strip().split(Util.DLM)
	    #self.idfData[self.coboughtItem[sc]] = float(idfValue)
