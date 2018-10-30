#
# Copyright (c) 2018, Edgewise Networks Inc. All rights reserved.
#

import os, sys
import gzip
from itertools import repeat
from Netflows import Netflow
from random import random, randint, sample
import multiprocessing
from datetime import datetime

def iterdir( dirname ):                                                                              
    try:                                                          
        for fname in sorted(map(os.path.join, repeat(dirname), os.listdir(dirname))):
            if os.path.isfile(fname):
                yield fname 
            elif os.path.isdir(fname):  
                for p in iterdir(fname):
                    yield p             
    except: 
        pass

def iterNetflows(fname):                                                                                     
    with gzip.open(fname, 'r') as f:                              
        for x in f:
            yield Netflow( *x.decode('utf8').strip().split('\t') )


def iterateData(pathname, siteId):
    dirname = pathname % siteId
    for fname in iterdir(dirname):
        if fname.endswith(".txt.gz"):
            for nf in iterNetflows(fname):
                yield nf

def iterateNetworkDataImpl(pathname, siteId, maxCount=None):
    cnt = 0
    dirname = pathname % siteId
    for fname in iterdir(dirname):
        if fname.endswith(".txt.gz"):
            for nf in iterNetflows(fname):
                if maxCount is not None and cnt >= maxCount:
                    print("iterated %i netflows - terminated" % cnt)
                    raise StopIteration
                if nf.inNetwork():
                    cnt += 1
                    yield nf
    print("iterated %i netflows - completed" % cnt)

def iterateNetworkData(pathname, siteId, maxCount=None):
    startTime = datetime.now().timestamp()
    it = iterateNetworkDataImpl(pathname, siteId, maxCount)
    nf = next(it)
    offset = startTime - nf.timestamp
    nf.timestamp = startTime
    yield nf
    for nf in it:
        nf.timestamp += offset
        yield nf
        

def processFile(fname):
    print("start processFile:", fname)
    return [nf for nf in iterNetflows(fname) if nf.inNetwork()]

def iterateNetworkDataParallel(pathname, siteId, maxCount=None):
    cnt = 0
    dirname = pathname % siteId
    it = [fname for fname in iterdir(dirname) if fname.endswith(".txt.gz")]
    numthreads = 8
    # create the process pool
    pool = multiprocessing.Pool(processes=numthreads)
    for netflowList in pool.imap(processFile, it, 1):
        if maxCount is not None and cnt >= maxCount:
            pool.terminate()
            break
        cnt += len(netflowList)
        for nf in netflowList:
            yield nf
    print("iterated %i netflows - completed" % cnt)
    

def iterateNetworkDataWithPortScanning(pathname, siteId, insertionFreq=0.01, maxCount=None):
    portScanner = "10.10.10.10"
    it = iterateNetworkData(pathname, siteId, maxCount)
    ipPortSet = set()
    dirname = pathname % siteId
    for nf in it:
        srcip = nf.srcip
        dstip = nf.dstip
        srcport = nf.srcport
        dstport = nf.dstport
        ipPortSet.add( (srcip, srcport) )
        ipPortSet.add( (dstip, dstport) )
        yield nf 
        if len(ipPortSet) > 100 and random() < insertionFreq:
            ts = nf.timestamp
            srcport = randint(0, 65536)
            dstip, dstport = sample(ipPortSet, 1)[0]
            flowCount = randint(1, 100)
            scanFlow = Netflow(ts, portScanner, srcport, dstip, dstport, flowCount)
            yield scanFlow
