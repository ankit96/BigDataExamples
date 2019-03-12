from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re

class DistributedGrep(MRJob):
    
    
    def __init__(self, *args, **kwargs):
        super(DistributedGrep, self).__init__(*args, **kwargs)
        self.regex = r"[mu]+"
       
    #defining steps 
    def steps(self):    
        return [
            MRStep(mapper=self.mapper
                  )

        ]

    #MapReduce Phase 1 : convert temperature data into city,day,temp,temp_count
    def mapper(self, _, line):
        (city,temp,timestamp) = line.split('|')
        if re.search(self.regex,city):
            yield city,(temp,timestamp)


if __name__ == '__main__':
    DistributedGrep.run()


# command : python counters.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar city_temperature.txt

