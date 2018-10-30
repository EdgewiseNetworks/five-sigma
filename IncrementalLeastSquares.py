#
# Copyright (c) 2018, Edgewise Networks Inc. All rights reserved.
#

inf = float('inf')

class ILS(object):
    """ Incrementally does linear least squares estimation for the 2-D case.
    """
    __slots__ = ('xbar', 'ybar', 'betaNumer', 'betaDenom', 'cnt')

    def __init__(self):
        self.xbar = 0
        self.ybar = 0
        self.betaNumer = 0
        self.betaDenom = 0
        self.cnt = 0

    def update(self, x, y):
        self.cnt += 1
        self.xbar += (x - self.xbar)/self.cnt
        self.ybar += (y - self.ybar)/self.cnt
        #self.betaNumer += (x - self.xbar) * (y - self.ybar)
        #self.betaDenom += (x - self.xbar) * (x - self.xbar)
        self.betaNumer += (x - self.xbar) * (y - self.ybar)
        self.betaDenom += (y - self.ybar) * (y - self.ybar)

    def getCount(self):
        return self.cnt

    def estimate(self):
        """
        :return: slope and intercept as a tuple. No estimate if only one datum has been seen.
        """
        if self.cnt < 2: return (0, 0)
        if self.betaDenom == 0:
            return (0, inf)
        slope = self.betaNumer / self.betaDenom
        #intercept = self.ybar - slope * self.xbar
        intercept = self.xbar - slope * self.ybar
        return (slope, intercept)
