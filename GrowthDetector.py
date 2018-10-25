#
# Copyright (c) 2018, Edgewise Networks Inc. All rights reserved.
#

from sys import float_info
from collections import defaultdict
import heapq
from datetime import datetime
from math import sqrt
from OnlineDeviation import Stdev
from Netflows import Netflow
from HLL import HyperLogLog
from NetflowDetector import NetflowDetector, timestampToDatetime
from heapq import heappush, heappop, heappushpop

maxfloat = float_info.max

class GrowthDetector(NetflowDetector):
    """ Compare current short for short's history for each key => discover if
        an app,host has become busier all of the sudden.
    
        (For example, at the 5-sigma level, if we want to avoid most false alarms.) 
        
        To do this, we create an HLL for each IP address, and add (ip+port)
        items to the HLL. (We also keep one for the total number of ip+port
        combinations we see.) We regularly check the cardinality for each
        IP address, and issue a warning when it's > 5 sigmas above 
        the expected number for all the HLLs.
    """
    __slots__ = ('shortCardDict', 'stdevDict', 'totalCount', 'topN')

    def __init__(self, sigmaCount=5, period=3600, topN=10):
        super().__init__(sigmaCount, period=period)
        self.topN = topN
        self.shortCardDict = defaultdict(lambda: HyperLogLog(16))
        self.stdevDict = defaultdict(Stdev)
        self.totalCount = 0
    
    def addNetflow(self, netflow):
        srcip = netflow.getSourceIpString()
        dst = netflow.getDestinationString()
        self.shortCardDict[ srcip ].add( dst )
        self.totalCount += 1
    
    def logOutput(self, key, result):
        """ key: an item being tracked
            result: bool -- True if starting above sigmaCount, False if ending above it.
        """ 
        dt = timestampToDatetime(self.lastTimestamp)
        if result:
            print("%s ::: IP address %s became an outlier for growth scanning." % (dt, key) )
        else:
            print("%s ::: IP address %s is no longer an outlier for growth scanning." % (dt, key) )
    
    def getOutliers(self):
        """ must return a dict of (key, sigmas > sigmaCount)
        """
        outliers = {}
        topN = self.topN
        for key, hll in self.shortCardDict.items():
            shortCount = hll.cardinality() 
            prevMean = self.stdevDict[key].getMean()
            prevStdev = self.stdevDict[key].getStdev()
            if shortCount >= prevMean + prevStdev * self.sigmaCount:
                sigs = (shortCount - prevMean) / prevStdev
                outliers[key] = sigs
            # update stdevDict, while we've got the information to do so.
            self.stdevDict[key].add(shortCount)
        # empty short-term HLLs
        for key in self.shortCardDict.keys():
            self.shortCardDict[key] = HyperLogLog(16)
        return outliers

    def getMeansAndStdDevs(self):
        return [(stdev.getMean(), stdev.getStdev()) for stdev in self.stdevDict.values()]
