#!/usr/bin/python
"""
count the number of measurements of each type
"""
import pandas as pd
import numpy as np
import sklearn as sk
import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr

class MRWeather(MRJob):

    def mapper(self, _, line):
        self.increment_counter('MrJob Counters','mapper-all',1)
        elements=line.split('\t')
        elements[0] = elements[0].replace('"','')
        out = (elements[0],1)
        yield out
        
    def reducer1(self, word, counts):
         yield (word,1) 
            
    def reducer2(self, word, counts):
        
        sumv = np.zeros(730)
        cov = np.zeros((730,730))       
        index=0
        data={}
        for vector in counts:
            data[index] = vector
            index = index + 1
        
        for i in range(0,index):
            sumv = sumv + np.array(data[i])
        sumlist = sumv.tolist()
        
        meanlist = [float(i)/index for i in sumlist]
        
        for i in range(0,index):
            temp = np.array(data[i])-np.array(meanlist)
            cov = cov + np.outer(temp,temp)
        cov = np.divide(cov,index)
        covmatrix = np.reshape(cov,(1,730*730))
        result1 = covmatrix.tolist()
        result2 = meanlist
        result3 = index
        
        try:
            U,D,V=np.linalg.svd(cov)
            total = sum(D)
            count = 0
            n=0
            for n in range(0,730):
                count+=D[n]
                if count/total>0.99:
                    break
            out = (word,[n+1,result1,result2,result3])
            yield out
        except Exception, e:
            yield ('error',[word,index])
        
                              
if __name__ == '__main__':
    MRWeather.run()