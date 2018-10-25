#
# Copyright (c) 2018, Edgewise Networks Inc. All rights reserved.
#

from math import sqrt

class Stdev(object):

    __slots__ = ('n', 'mean', 'm2')

    def __init__(self):
        self.n = 0
        self.mean = 0
        self.m2 = 0

    def add(self, x):
        self.n += 1
        delta = x - self.mean
        self.mean += delta/self.n
        delta2 = x - self.mean
        self.m2 += delta * delta2

    def getMean(self):
        return self.mean

    def getVariance(self):
        if self.m2 < 2:
            return float('nan')
        else:
            return self.m2 / (self.n - 1)

    def getStdev(self):
        return sqrt(self.getVariance())


def test():
    from random import shuffle
    l = [1, 2, 3, 4, 5, 6, 7]
    s = Stdev()
    for x in l:
        s.add(x)
    assert abs(s.getMean() - 4.0) < 0.00001
    assert abs(s.getStdev() - 2.16025) < 0.00001
    shuffle(l)
    s2 = Stdev()
    for x in l:
        s2.add(x)
    assert abs(s2.getMean() - 4.0) < 0.00001
    assert abs(s2.getStdev() - 2.16025) < 0.00001
