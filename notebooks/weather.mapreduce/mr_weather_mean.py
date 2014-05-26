#!/usr/bin/python
"""
count the number of measurements of each type
"""
import sys
import pandas as pd
import numpy as np
import sklearn as sk
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr

class MRWeather(MRJob):

    def mapper(self, _, line):
        try:
            self.increment_counter('MrJob Counters','mapper-all',1)
            elements=line.split(',')
            if elements[0]=='station':
                out=('header',[1])
            else:
                for i in range(3,len(elements)):
                    if elements[i]=='':
                        elements[i] = 0
                    else:
                        elements[i] = int(elements[i])
                out=('key',elements[3:])
        except Exception, e:
            stderr.write('Error in line:\n'+line)
            stderr.write(e)
            self.increment_counter('MrJob Counters','mapper-error',1)
            out=('error',[1])

        finally:
            yield out
   

    def reducer(self, word, counts):
        
        self.increment_counter('MrJob Counters','reducer',1)
        count = 0
        for vector in counts:
            count = count+1
        sumvec=np.zeros(365)
        sumvec2=np.zeros(365)
        matr=np.zeros((365,365))
        data={}
        for n in range(1,count+1):
            sumvec=sumvec+np.array(data[n])
        if station=='sum':
            sumlis=sumvec.tolist()
            meanlis=[n/tt for n in sumlis]
            yield('sum',meanlis)
        for n in range(1,tt+1):
            subtarr=np.array(numvec)-np.array(meanlis)
            matr+=np.outer(subtarr,subtarr)
        if station=='key':
            yield('PCA',matr.tolist())

if __name__ == '__main__':
    MRWeather.run()