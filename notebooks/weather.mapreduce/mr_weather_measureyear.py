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
            if elements[1] == 'TMAX' or elements[1]=='TMIN':
                if len(elements)==368:
                    out = (elements[0],1)
                    yield out
            
    def combiner(self, word, counts):
        self.increment_counter('MrJob Counters','combiner',1)
        yield (word, sum(counts))
        #l_counts=[c for c in counts]  # extract list from iterator
        #S=sum(l_counts)
        #logfile.write('combiner '+word+' ['+','.join([str(c) for c in l_counts])+']='+str(S)+'\n')
        #yield (word, S)

    def reducer(self, word, counts):
        self.increment_counter('MrJob Counters','reducer',1)
        yield (word, sum(counts))
        #l_counts=[c for c in counts]  # extract list from iterator
        #S=sum(l_counts)
        #logfile.write('reducer '+word+' ['+','.join([str(c) for c in l_counts])+']='+str(S)+'\n')
        #yield (word, S)

if __name__ == '__main__':
    MRWeather.run()