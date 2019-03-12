from mrjob.job import MRJob
from mrjob.step import MRStep
from random import randint

class SimpleRandomSampling(MRJob):
    
    
    def __init__(self, *args, **kwargs):
        super(SimpleRandomSampling, self).__init__(*args, **kwargs)
        self.percentage = 30
       
    #defining steps 
    def steps(self):    
        return [
            MRStep(mapper=self.mapper
                  )

        ]

    #MapReduce Phase 1 : convert temperature data into city,day,temp,temp_count
    def mapper(self, _, line):
        (city,temp,timestamp) = line.split('|')
        if randint(1,101)<=self.percentage:
            yield city,(temp,timestamp)


if __name__ == '__main__':
    SimpleRandomSampling.run()


# command : python SimpleRandomSampling.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar city_temperature.txt

