#
# Copyright (c) 2018, Edgewise Networks Inc. All rights reserved.
#

import ipaddress

class NetflowOrig(object):

    __slots__ = ('timestamp', 'srcip', 'srcport', 'dstip', 'dstport', 'flows')
    
    def __init__(self, timestamp, srcip, srcport, dstip, dstport, flows, javaTimestamp=True):
        if javaTimestamp:
            self.timestamp = int(timestamp)/1000.0
        else:
            self.timestamp = int(timestamp)
        self.srcip = ipaddress.ip_address(srcip)
        self.srcport = int(srcport)
        self.dstip = ipaddress.ip_address(dstip)
        self.dstport = int(dstport)
        self.flows = int(flows)

    def inNetwork(self):
        isIn = True
        def isInside(ipaddr):
             return ipaddr.is_link_local or ipaddr.is_loopback
        return (not isInside(self.srcip)) and (not isInside(self.dstip))
    
    def __repr__(self): 
       return 'Netflow(ts=%f, src=%s:%i, dst=%s:%i, flows=%i)' % (self.timestamp, self.srcip, 
              self.srcport, self.dstip, self.dstport, self.flows)
    
    def getSourceIpString(self):
        return str(self.srcip)
    
    def getDestinationIpString(self):
        return str(self.dstip)
    
    def getSourceString(self):
        "%s:%i" % (self.srcip, self.srcport)
    
    def getDestinationString(self):
        return "%s:%i" % (self.dstip, self.dstport)

def isInside(ipaddr):
    return ipaddr.startswith('169.254.') or \
           ipaddr.startswith('127.0.0.') or \
           ipaddr.startswith('fe80:') or \
           ipaddr == "0:0:0:0:0:0:0:1" or \
           ipaddr == "::1"

class Netflow(object):

    __slots__ = ('timestamp', 'srcip', 'srcport', 'dstip', 'dstport', 'flows', 'isOverNetwork')
    
    def __init__(self, timestamp, srcip, srcport, dstip, dstport, flows, javaTimestamp=True):
        if javaTimestamp:
            self.timestamp = int(timestamp)/1000.0
        else:
            self.timestamp = int(timestamp)
        self.srcip = srcip
        self.srcport = srcport
        self.dstip = dstip
        self.dstport = dstport
        self.flows = int(flows)
        self.isOverNetwork = (not isInside(self.srcip)) and (not isInside(self.dstip))

    def inNetwork(self):
        return self.isOverNetwork
    
    def __repr__(self): 
       return 'Netflow(ts=%f, src=%s:%s, dst=%s:%s, flows=%i)' % (self.timestamp, self.srcip, 
              self.srcport, self.dstip, self.dstport, self.flows)
    
    def getSourceIpString(self):
        return self.srcip
    
    def getDestinationIpString(self):
        return self.dstip
    
    def getSourceString(self):
        #return "%s:%s" % (self.srcip, self.srcport)
        return (self.srcip, self.srcport)
    
    def getDestinationString(self):
        return "%s:%s" % (self.dstip, self.dstport)
        #return (self.dstip, self.dstport)