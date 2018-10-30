#
# Copyright (c) 2018, Edgewise Networks Inc. All rights reserved.
#

from sys import float_info
from collections import defaultdict
from datetime import datetime
from math import sqrt
from Netflows import Netflow

maxfloat = float_info.max

class TimePeriods(object):

    __slots__ = ('timestampPairs', 'currentStart')
        
    def __init__(self, currStart=None):
        self.timestampPairs = {}
        self.currentStart = currStart
        
    def end(self, e):
        if self.currentStart is None:
            raise Exception("currentStart not defined")
        self.timestampPairs[self.currentStart] = e
        self.currentStart = None
        
    def start(self, s):
        if self.currentStart is not None:
            raise Exception("currentStart already defined")
        self.currentStart = s
    
    def active(self):
        return self.currentStart is not None
    

class TimePeriodMap(object):

    __slots__ = ('map')
    
    def __init__(self):
        self.map = defaultdict(TimePeriods)
    
    def __len__(self):
        return len(self.map)
    
    def update(self, itemSet, timestamp):
        result = {}
        for item in itemSet:
            if item not in self.map:
                self.map[item].start(timestamp)
                result[item] = True
            elif not self.map[item].active():
                self.map[item].start(timestamp)
                result[item] = True
        for key, tp in self.map.items():
            if key not in itemSet and tp.active():
                tp.end(timestamp)
                result[key] = False
        return result
    
    def getActives(self):
        return list( k for k, v in self.map.items() if v.active() )
    
    def keys(self):
        return self.map.keys()

def timestampToDatetime(timestamp):
    return datetime.fromtimestamp(timestamp)


class NetflowDetector(object):
    """ We want to find an IP address pair that's scanning a large number of
        ip address + port combinations. 
    
        (For example, at the 5-sigma level, if we want to avoid most false alarms.) 
        
        To do this, we create an HLL for each IP address, and add (ip+port)
        items to the HLL. (We also keep one for the total number of ip+port
        combinations we see.) We regularly check the cardinality for each
        IP address, and issue a warning when it's > 5 sigmas above 
        the expected number for all the HLLs.
    """
    __slots__ = ('sigmaCount', 'period', 'lastTimestamp', 'checkCount', 'timePeriodMap')

    def __init__(self, sigmaCount=5, period=600):
        self.sigmaCount = sigmaCount
        self.period = period
        self.lastTimestamp = None
        self.checkCount = 0
        self.timePeriodMap = TimePeriodMap()
        
    def addNetflowIterator(self, it):
        # first time
        netflow = next(it)
        self.addNetflow(netflow)
        self.lastTimestamp = netflow.timestamp 
        # the remaining times
        while True:
            try: netflow = next(it)
            except StopIteration:
                break
            self.addNetflow(netflow)
            self.checkNetflow(netflow.timestamp)
    
    def addNetflow(self, netflow):
        raise Exception("Implement in subclass")
        
    def checkNetflow(self, netflowTimestamp):
        if netflowTimestamp - self.lastTimestamp >= self.period:
            self.lastTimestamp = netflowTimestamp
            self.check()
    
    def getOutliers(self):
        """ must return a dict of (key, sigmas > sigmaCount)
        """
        raise Exception("Implement in subclass")
    
    def logOutput(self, key, result):
        """ key: an item being tracked
            result: bool -- True if starting above sigmaCount, False if ending above it.
        """ 
        raise Exception("Implement in subclass")
    
    def check(self):
        self.checkCount += 1
        if self.checkCount % 1000 == 0:
            print("%s checkCount = %i" % (type(self).__name__, self.checkCount))
        extremeDict = self.getOutliers()
        extremeSet = set(extremeDict.keys())
        key2result = self.timePeriodMap.update(extremeSet, self.lastTimestamp)
        for key, result in key2result.items():
            self.logOutput(key, result)
    
    def getExtremes(self):
        return self.timePeriodMap.getActives()
    
    def getExtremeCounts(self):
        return {"extreme": len(self.getExtremes()), 
                "previously extreme": len(self.timePeriodMap) }
