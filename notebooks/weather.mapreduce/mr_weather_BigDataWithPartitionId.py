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
        elements=line.split(',')
        if len(elements)==731:
            elements[0] = elements[0].replace('[','')
            elements[0] = elements[0].replace('"','')
            temp = elements[1].split('\t')
            elements[1].replace('[','')
            elements[1] = temp[1].replace('[','')
            elements[0] = elements[0].replace('[','')
            elements[len(elements)-1] = elements[len(elements)-1].replace(']','')
            for i in range(1,len(elements)):
                elements[i] = int(elements[i])
            out = (elements[0],elements[1:])
            yield out
        if len(elements)==2:
            out = (elements[0],[elements[1]])
            yield out
        
    def reducer(self, word, counts):
        self.increment_counter('MrJob Counters','reducer',1)
        
        partitionkey = -1
        index = 0
        data = {}
        for vector in counts:
            if len(vector)!=1:
                data[index] = vector
                index = index + 1
            if len(vector)==1:
                partitionkey = vector[0]
        for i in range(0,index):
            out = (partitionkey,data[i])
            yield out
        
        #l_counts=[c for c in counts]  # extract list from iterator
        #S=sum(l_counts)
        #logfile.write('reducer '+word+' ['+','.join([str(c) for c in l_counts])+']='+str(S)+'\n')
        #yield (word, S)

if __name__ == '__main__':
    MRWeather.run()