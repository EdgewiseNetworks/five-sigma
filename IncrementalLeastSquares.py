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
        self.betaNumer += (x - self.xbar) * (y - self.ybar)
        self.betaDenom += (x - self.xbar) * (x - self.xbar)

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
        intercept = self.ybar - slope * self.xbar
        return (slope, intercept)

from math import sqrt

class UpdatingSimpleLinearRegression(object):
    """
        xavg: average of previous x, if no previous sample, set to 0
        yavg: average of previous y, if no previous sample, set to 0
        Sxy: covariance of previous x and y, if no previous sample, set to 0
        Sx: variance of previous x, if no previous sample, set to 0
        n: number of previous samples
    """

    def __init__(self):
        self.xavg = 0
        self.yavg = 0
        self.Sxy = 0
        self.Sx = 0
        self.n = 0
    
    def update(self, new_x, new_y):
        """ new incoming data x, y 
        """
        new_n = self.n + 1
        new_x_avg = (self.xavg * self.n + new_x)/new_n
        new_y_avg = (self.yavg * self.n + new_y)/new_n
        if self.n > 0:
            x_star = (self.xavg*sqrt(self.n) + new_x_avg*sqrt(new_n)) / \
                     (sqrt(self.n)+sqrt(new_n))
            y_star = (self.yavg*sqrt(self.n) + new_y_avg*sqrt(new_n)) / \
                     (sqrt(self.n)+sqrt(new_n))
        elif self.n == 0:
            x_star = new_x_avg
            y_star = new_y_avg
        else:
            raise ValueError
        self.Sx += (new_x-x_star)**2
        self.Sxy += (new_x-x_star) * (new_y-y_star)
        self.n = new_n
        self.xavg = new_x_avg
        self.yavg = new_y_avg
    
    def estimate(self):
        if self.Sx == 0:
            return (0, inf)
        slope = self.Sxy/self.Sx
        intercept = self.yavg - slope * self.xavg
        return (slope, intercept)
        
#     https://stackoverflow.com/questions/52070293/efficient-online-linear-regression-algorithm-in-python
#
#     def lr(x_avg,y_avg,Sxy,Sx,n,new_x,new_y):
#         """
#         x_avg: average of previous x, if no previous sample, set to 0
#         y_avg: average of previous y, if no previous sample, set to 0
#         Sxy: covariance of previous x and y, if no previous sample, set to 0
#         Sx: variance of previous x, if no previous sample, set to 0
#         n: number of previous samples
#         new_x: new incoming 1-D numpy array x
#         new_y: new incoming 1-D numpy array x
#         """
#         new_n = n + len(new_x)
# 
#         new_x_avg = (x_avg*n + np.sum(new_x))/new_n
#         new_y_avg = (y_avg*n + np.sum(new_y))/new_n
# 
#         if n > 0:
#             x_star = (x_avg*np.sqrt(n) + new_x_avg*np.sqrt(new_n))/(np.sqrt(n)+np.sqrt(new_n))
#             y_star = (y_avg*np.sqrt(n) + new_y_avg*np.sqrt(new_n))/(np.sqrt(n)+np.sqrt(new_n))
#         elif n == 0:
#             x_star = new_x_avg
#             y_star = new_y_avg
#         else:
#             raise ValueError
# 
#         new_Sx = Sx + np.sum((new_x-x_star)**2)
#         new_Sxy = Sxy + np.sum((new_x-x_star).reshape(-1) * (new_y-y_star).reshape(-1))
# 
#         beta = new_Sxy/new_Sx
#         alpha = new_y_avg - beta * new_x_avg
#         return new_Sxy, new_Sx, new_n, alpha, beta, new_x_avg, new_y_avg

