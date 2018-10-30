from collections import deque

inf = float('inf')

class AbstractWindowOperation(object):

    __slots__ = ('dq', 'maxlen', 'cnt')
    
    def __init__(self, maxlen):
        self.dq = deque()
        self.maxlen = maxlen
        self.cnt = 0
    
    def add(self, item):
        if len(self.dq) >= self.maxlen:
            self.dq.popleft()
        self.dq.append(item)
        self.cnt += 1
    
    def estimate(self):
        raise Exception("implement in subclass")

class AverageWindow(AbstractWindowOperation):

    def __init__(self, maxlen=5):
        super().__init__(maxlen)
    
    def estimate(self):
        return sum(self.dq)/len(self.dq)

class SlopeWindow(AbstractWindowOperation):

    """ remember we're adding to the Y's here
    """

    def __init__(self, maxlen=5):
        super().__init__(maxlen)
    
    def estimate(self):
        if self.cnt < 2: 
            return inf
        xs = list(range(self.cnt - len(self.dq) + 1, self.cnt + 1))
        xbar = sum(xs)/len(xs)
        ybar = sum(self.dq)/len(self.dq)
        numer = 0
        denom = 0
        for x, y in zip(xs, self.dq):
            numer += (x-xbar) * (y-ybar)
            denom += (x-xbar) * (x-xbar)
        if denom == 0:
            return inf
        #print(xs, self.dq, xbar, ybar, numer, denom)
        return numer / denom


def testAverage():
    import random
    l = list(range(1, 12))
    aw = AverageWindow(11)
    for _ in range(10):
        random.shuffle(l)
        for i in l:
            aw.add(i)
        avg = aw.calculate()
        #print(avg)
        assert abs(avg - 6.0) < 0.000001

def testSlope():
    sw = SlopeWindow(10)
    yval = 50
    decr = 2
    for _ in range(10):
        for _ in range(10):
            sw.add(yval)
            yval -= decr
        slope = sw.calculate()
        #print(slope)
        assert abs(slope + decr) < 0.000001
        decr /= 2
        