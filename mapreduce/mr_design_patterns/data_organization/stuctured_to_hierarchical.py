from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class Structured_to_Hierarical(MRJob):
    
    
    def __init__(self, *args, **kwargs):
        super(Structured_to_Hierarical, self).__init__(*args, **kwargs)
        
       
    #defining steps 
    def steps(self):    
        return [
            MRStep(mapper=self.mapper
                ,reducer_init=self.reducer_init
                ,reducer=self.reducer
                ,reducer_final=self.reducer_final
                  )

        ]

    #MapReduce Phase 1 : convert temperature data into city,day,temp,temp_count
    def mapper(self, _, line):
        inp_list = line.split('|')
        if len(inp_list)==3:
            (city,temp,timestamp) = inp_list
            yield city,('t',temp,timestamp)
        else:
            (country,city) = inp_list
            yield city,('c',country)

    def reducer_init(self):
        self.temp_stats = dict()
        self.country_city = tuple()


    def reducer(self,city,city_stats):

        for stats in city_stats:
            if stats[0]=='t':
                if stats[1] in self.temp_stats:
                    self.temp_stats[stats[1]].append(stats[2])
                else:
                    self.temp_stats[stats[1]] = [stats[2]]
            else:
                self.country_city = (stats[1],city)


    def reducer_final(self):
        if self.country_city[1]=='Mumbai':
            x = json.dumps({self.country_city[0]:{self.country_city[1]:self.temp_stats}})
            #print x
            yield None,x

   
        

if __name__ == '__main__':
    Structured_to_Hierarical.run()


# command : python counters.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar city_temperature.txt

