#!/usr/bin/python
"""
count the number of measurements of each type
"""
import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr

class MRWeather(MRJob):

    def mapper(self, _, line):
        self.increment_counter('MrJob Counters','mapper-all',1)
        elements=line.split('\t')
        temp = elements[1].split(',')
        temp[0] = temp[0].replace('[','')
        temp[len(temp)-1] = temp[len(temp)-1].replace(']','')
        for i in range(0,len(temp)):
            temp[i] = int(temp[i])
        out = (elements[0],temp)
        yield out
        
            
    def reducer(self, word, counts):
        
        sumv = np.zeros(730)
        cov = np.zeros((730,730))
        
       
        index=0
        data={}
        for vector in counts:
            data[index] = vector
            index = index + 1
        
        for i in range(0,index):
            sumv = sumv + np.array(data[n])
        sumlist = sumv.tolist()
        
        meanlist = [float(i)/index for i in sunlist]
        
        for i in range(0,index):
            temp = np.array(data[i])-np.array(meanlist)
            cov = cov + np.outer(temp,temp)
        cov = np.divide(cov,index)
        print cov
        U,D,V=np.linalg.svd(cov)
        count = 0
        total = sum(D)
        n=0
        for n in range(0,730):
            count+=D[n]
            if count/total>0.99:
                break
        out = (word,n+1)
        out = (word,index)
        yield out
                              
        
if __name__ == '__main__':
    MRWeather.run()