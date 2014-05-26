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
        if elements[1]=='TMAX' and len(elements)==368:
            self.increment_counter('MrJob Counters','TMAX',1)
            countZero = 0
            for i in range(2,len(elements)):
                if elements[i]=='':
                    countZero = countZero + 1
                    elements[i] = 0
                else:
                    elements[i] = int(elements[i])
            if countZero <= 365*0.1:
                out=([elements[0],elements[2]],['TMAX',elements[3:]])
                yield out
        else:
            if elements[1]=='TMIN' and len(elements)==368:
                self.increment_counter('MrJob Counters','TMIN',1)
                countZero = 0
                for i in range(2,len(elements)):
                    if elements[i]=='':
                        countZero = countZero + 1
                        elements[i] = 0
                    else:
                        elements[i] = int(elements[i])
                if countZero <= 365*0.1:
                    out=([elements[0],elements[2]],['TMIN',elements[3:]])
                    yield out
        
    def reducer(self, word, counts):
        self.increment_counter('MrJob Counters','reducer',1)
        if word == 'TMIN' or word =='TMAX':
            yield(word,sum(counts))
        else:
            count = 0
            result=[]
            for vector in counts:
                count = count+1
                if vector[0]=='TMIN':
                    result = result + vector[1]
                else:
                    if vector[0]=='TMAX':
                        result = vector[1]+ result
            if count==2:
                out =(word, result)
                yield out
      
        
        #l_counts=[c for c in counts]  # extract list from iterator
        #S=sum(l_counts)
        #logfile.write('reducer '+word+' ['+','.join([str(c) for c in l_counts])+']='+str(S)+'\n')
        #yield (word, S)

if __name__ == '__main__':
    MRWeather.run()