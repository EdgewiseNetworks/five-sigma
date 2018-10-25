#
# Copyright (c) 2018, Edgewise Networks Inc. All rights reserved.
#

class CompositeNetflowDetector(object):
    """ We want to find an IP address pair that's scanning a large number of
        ip address + port combinations. 
    
        (For example, at the 5-sigma level, if we want to avoid most false alarms.) 
        
        To do this, we create an HLL for each IP address, and add (ip+port)
        items to the HLL. (We also keep one for the total number of ip+port
        combinations we see.) We regularly check the cardinality for each
        IP address, and issue a warning when it's > 5 sigmas above 
        the expected number for all the HLLs.
    """
    __slots__ = ('detectors')

    def __init__(self):
        self.detectors = []
        
    def addDetector(self, detector):
        self.detectors.append(detector)
        
    def addNetflowIterator(self, it):
        # first time
        netflow = next(it)
        self.addNetflow(netflow)
        for detector in self.detectors:
            detector.lastTimestamp = netflow.timestamp
        # the remaining times
        while True:
            try: netflow = next(it)
            except StopIteration:
                break
            self.addNetflow(netflow)
            self.checkNetflow(netflow.timestamp)
        
    def addNetflow(self, netflow):
        for detector in self.detectors:
            detector.addNetflow(netflow)
    
    def checkNetflow(self, netflowTimestamp):
        for detector in self.detectors:
            detector.checkNetflow(netflowTimestamp)

