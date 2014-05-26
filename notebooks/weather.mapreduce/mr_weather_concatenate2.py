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
        try:
            self.increment_counter('MrJob Counters','mapper-all',1)
            elements=line.split(',')
            if elements[0]=='station':
                out=('header',1)
            else:
                if elements[1]=='TMIN':
                    length = len(elements)
                    for i in range(3,length):
                        if elements[i]=='':
                            elements[i] = 0
                        else:
                            elements[i] = int(elements[i])
                    out=([elements[0],elements[2]],elements[3:])
                else:
                    out=(['not max','not max'],elements[3:])

        except Exception, e:
            stderr.write('Error in line:\n'+line)
            stderr.write(e)
            self.increment_counter('MrJob Counters','mapper-error',1)
            out=('error',1)

        finally:
            yield out
            
    def combiner(self,word, counts):
        self.increment_counter('MrJob Counters','combiner',1)
        yield (word, max(counts))
        #l_counts=[c for c in counts]  # extract list from iterator
        #S=sum(l_counts)
        #logfile.write('combiner '+word+' ['+','.join([str(c) for c in l_counts])+']='+str(S)+'\n')
        #yield (word, S)
        
    def reducer(self, word, counts):
        self.increment_counter('MrJob Counters','reducer',1)
        yield (word, max(counts))
        #l_counts=[c for c in counts]  # extract list from iterator
        #S=sum(l_counts)
        #logfile.write('reducer '+word+' ['+','.join([str(c) for c in l_counts])+']='+str(S)+'\n')
        #yield (word, S)

if __name__ == '__main__':
    MRWeather.run()