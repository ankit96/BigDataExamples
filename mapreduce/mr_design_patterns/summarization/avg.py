from mrjob.job import MRJob
from mrjob.step import MRStep
import  time



class Avg(MRJob):

    def __init__(self, *args, **kwargs):
        super(Avg, self).__init__(*args, **kwargs)


    def steps(self,args = None):
        return [
            MRStep(mapper=self.map_temp,
                    combiner = self.combine_local_sum_count_temp,
                   reducer=self.reduce_global_sum_count_temp),
            MRStep(reducer=self.sorting_reducer)
        ]

    def map_temp(self, _, line):
        (city,temp,timestamp) = line.split('|')
        #output (city,date), temperature
        yield (city,self.get_day_from_timestamp(timestamp)), int(temp)

    def combine_local_sum_count_temp(self, city_day, data):
        sumx = 0
        countx = 0
        for temperature in data:
            sumx += temperature
            countx+=1


        #output (city,date),(sum_temperature,count)
        yield city_day,(sumx,countx)

    def reduce_global_sum_count_temp(self, city_day, data):

        sumx = 0
        countx = 0

        for temperature in data:
            sumx += temperature[0]
            countx += temperature[1]


        #yield city,(date,(avg_temperature))
        yield city_day[0],(city_day[1],(sumx*1.0)/countx)

    def sorting_reducer(self, city, day_stats):

        day_stats = list(day_stats) #safe to do until date range is assumed to be small
        day_stats = sorted(day_stats, key=lambda day_stats: day_stats[0])

        for data in day_stats:
            date = self.convert_epoch(data[0])
            #output city,date,min_temp_max_temp,count
            print city,date,data[1]



    ###################################################################################
    ## utility functions
    def get_day_from_timestamp(self,timestamp):
        one_day = 60*60*24
        return int((int(timestamp)//one_day)*one_day)

    def convert_epoch(self,epoch):
        return time.strftime('%Y-%m-%d', time.localtime(epoch))

    
if __name__ == '__main__':
    Avg.run()
