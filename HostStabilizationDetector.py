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
from IncrementalLeastSquares import ILS, UpdatingSimpleLinearRegression

maxfloat = float_info.max
inf = float('inf')

class HostStabilizationDetector(NetflowDetector):
    """ Keep track of observed ip+port comms for each ip address, and determine
        when it should have fallen permanently into a stable, "frozen" state.
        We assume an exponential fall-off in the number of ip+port combinations
        left to be seen. From this, we estimate:
        
        - avg * slope = N_rem
        
        running average and slope with points updated at each time slice
            
        To do this, we have an HLL for each IP address to keep track of the total
        number of ip+port comms. At each check time, this count is estimated, and 
        that estimate is added to a incremental LSTQ estimator for each IP address
        (to get the slope) and an incremental averaging calculator.
        
        This allows us to estimate the remaining IP+ports for this host, and
        also -- if we want -- to determine when the host is NEVER likely to
        stabilize, or "freeze", or when the exponential model is simply
        not appropriate.
    """
    __slots__ = ('longCardDict', 'slopeDict', 'avgDict', 'totalCount', 
                 'updatePeriod', 'tolerance', 'frozenHosts', 'everFrozen')

    def __init__(self, sigmaCount=5, period=86400, tolerance=0.001):
        super().__init__(sigmaCount, period=period)
        self.longCardDict = defaultdict(lambda: HyperLogLog(16))
        self.slopeDict = defaultdict(lambda: ILS())
        #self.slopeDict = defaultdict(lambda: UpdatingSimpleLinearRegression())
        self.avgDict = defaultdict(lambda: Stdev())
        self.totalCount = 0
        self.updatePeriod = 0
        self.tolerance = tolerance
        self.frozenHosts = set()
        self.everFrozen = HyperLogLog(16)
    
    def addNetflow(self, netflow):
        srcip = netflow.getSourceIpString()
        dst = netflow.getDestinationString()
        self.longCardDict[ srcip ].add( dst )
        self.totalCount += 1
    
    def check(self):
        self.checkCount += 1
        if self.checkCount % 1000 == 0:
            print("%s checkCount = %i" % (type(self).__name__, self.checkCount))
        extremeDict = self.getOutliers()
        for key, result in extremeDict.items():
            self.logOutput(key, result)
    
    def logOutput(self, key, result):
        """ key: an item being tracked
            result: bool -- True if starting above sigmaCount, False if ending above it.
        """ 
        dt = timestampToDatetime(self.lastTimestamp)
        if result:
            print("%s ::: IP address %s became frozen." % (dt, key) )
        else:
            print("%s ::: IP address %s is no longer frozen!" % (dt, key) )
    
    def getOutliers(self):
        """ must return a dict of (key, N_rem if > tolerance)
        """
        outliers = {}
        self.updatePeriod += 1
        for key, hll in self.longCardDict.items():
            N_obs = hll.cardinality()
            # add to slope and average
            self.avgDict[key].add(N_obs)
            self.slopeDict[key].update(N_obs, self.updatePeriod)
            # get new slope and average
            avg = self.avgDict[key].getMean()
            slope, intercept = self.slopeDict[key].estimate()
            N_rem = slope * avg
            if intercept == inf:
                continue
            elif N_rem < 0:
                # slope is positive; N_rem is negative
                print(key, "has a negative slope, and N_rem estimate is negative. Slope =", slope)
            elif N_rem <= self.tolerance and key not in self.frozenHosts:
                self.frozenHosts.add(key)
                outliers[key] = True
                self.everFrozen.add(key)
            elif N_rem > self.tolerance and key in self.frozenHosts:
                self.frozenHosts.remove(key)
                outliers[key] = False
        return outliers

    def getCardinalities(self):
        return sorted(hll.cardinality() for hll in self.longCardDict.values())
    
    def getMeans(self):
        return sorted(stdev.getMean() for stdev in self.avgDict.values())

    def getSlopes(self):
        return sorted(reg.estimate()[0] for reg in self.slopeDict.values())
    
    def getExtremes(self):
        return frozenset(self.longCardDict.keys()) - self.frozenHosts
    
    def getExtremeCounts(self):
        return len(self.getExtremes()), int(self.everFrozen.cardinality())
