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

class IpPortScanDetector(NetflowDetector):
    """ We want to find an IP address pair that's scanning a large number of
        ip address + port combinations. 
    
        (For example, at the 5-sigma level, if we want to avoid most false alarms.) 
        
        To do this, we create an HLL for each IP address, and add (ip+port)
        items to the HLL. (We also keep one for the total number of ip+port
        combinations we see.) We regularly check the cardinality for each
        IP address, and issue a warning when it's > 5 sigmas above 
        the expected number for all the HLLs.
    """
    __slots__ = ('cardinalityDict', 'totalCard', 'totalCount', 'topN')

    def __init__(self, sigmaCount=5, period=600, topN=10):
        super().__init__(sigmaCount, period=period)
        self.topN = topN
        self.cardinalityDict = defaultdict(lambda: HyperLogLog(16))
        self.totalCard = HyperLogLog(16)
        self.totalCount = 0
    
    def addNetflow(self, netflow):
        srcip = netflow.getSourceIpString()
        dst = netflow.getDestinationString()
        self.cardinalityDict[ srcip ].add( dst )
        self.totalCard.add( dst )
        self.totalCount += 1
    
    def getOutliersAll(self):
        """ must return a dict of (key, sigmas if > sigmaCount)
        """
        outliers = {}
        s = Stdev()
        for key, hll in self.cardinalityDict.items():
            cnt = hll.cardinality()
            s.add(cnt)
        mean = s.getMean()
        stdv = s.getStdev()
        for key, hll in self.cardinalityDict.items():
            cnt = hll.cardinality()
            if cnt > mean + self.sigmaCount * stdv:
                sigs = (cnt - mean) / stdv
                outliers[key] = sigs
        return outliers
    
    def logOutput(self, key, result):
        """ key: an item being tracked
            result: bool -- True if starting above sigmaCount, False if ending above it.
        """ 
        dt = timestampToDatetime(self.lastTimestamp)
        if result:
            print("%s ::: IP address %s became an outlier for IP port scanning." % (dt, key) )
        else:
            print("%s ::: IP address %s is no longer an outlier for IP port scanning." % (dt, key) )
    
    def getOutliers(self):
        """ must return a dict of (key, sigmas > sigmaCount)
        """
        outliers = {}
        s = Stdev()
        h = []
        topN = self.topN
        for key, hll in self.cardinalityDict.items():
            cnt = hll.cardinality()
            s.add(cnt)
            if len(h) < topN:
                heapq.heappush( h, (cnt, key) )
            else:
                heapq.heappushpop( h, (cnt, key) )
        mean = s.getMean()
        stdv = s.getStdev()
        for cnt, key in h:
            if cnt > mean + self.sigmaCount * stdv:
                sigs = (cnt - mean) / stdv
                outliers[key] = sigs
        for key in self.timePeriodMap.getActives():
            cnt = hll.cardinality()
            if cnt > mean + self.sigmaCount * stdv:
                sigs = (cnt - mean) / stdv
                outliers[key] = sigs
        return outliers
    
    def getCardinalities(self):
        return sorted(hll.cardinality() for hll in self.cardinalityDict.values())