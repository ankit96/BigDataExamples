from mrjob.job import MRJob
from mrjob.step import MRStep
import  time


# Couldn/t use class objects as intermediate values of mapper and reducer steps since it is not serializable ,
# Also using namedtuple didnt work since it gets converted to list by default for serializability(Reason assumed)
# class day_temp_stats():
#     def __init__(self,min_temp,max_temp,count,date = None):
#         self.min_temp = min_temp
#         self.max_temp = max_temp
#         self.count = count
#         self.date = date


class MinMaxCount(MRJob):

    def __init__(self, *args, **kwargs):
        super(MinMaxCount, self).__init__(*args, **kwargs)


    def steps(self,args = None):
        return [
            MRStep(mapper=self.map_temp,
                    combiner = self.combine_local_min_max_count_temp,
                   reducer=self.reduce_global_min_max_count_temp),
            MRStep(reducer=self.sorting_reducer)
        ]

    def map_temp(self, _, line):
        (city,temp,timestamp) = line.split('|')
        #output (city,date), temperature
        yield (city,self.get_day_from_timestamp(timestamp)), int(temp)

    def combine_local_min_max_count_temp(self, city_day, data):
        minx = 10000
        maxx = -10000
        count = 0

        for temperature in data:
            minx = min(temperature,minx)
            maxx = max(temperature,maxx)
            count+=1

        if minx<10000 and maxx>-10000:
            #output (city,date),(min_temperature,max_temperature,count)
            yield city_day,(minx,maxx,count)

    def reduce_global_min_max_count_temp(self, city_day, data):

        minx = 10000
        maxx = -10000
        count = 0

        for temperature in data:
            minx = min(temperature[0],minx)
            maxx = max(temperature[1],maxx)
            count+=temperature[2]

        if minx<10000 and maxx>-10000:
            #yield city,(date,(min_temperature,max_temperature,count))
            yield city_day[0],(city_day[1],(minx,maxx,count))

    def sorting_reducer(self, city, day_stats):

        day_stats = list(day_stats) #safe to do until date range is assumed to be small
        day_stats = sorted(day_stats, key=lambda day_stats: day_stats[0])

        for data in day_stats:
            date = self.convert_epoch(data[0])
            #output city,date,min_temp_max_temp,count
            print city,date,data[1][0],data[1][1],data[1][2]



    ###################################################################################
    ## utility functions
    def get_day_from_timestamp(self,timestamp):
        one_day = 60*60*24
        return int((int(timestamp)//one_day)*one_day)

    def convert_epoch(self,epoch):
        return time.strftime('%Y-%m-%d', time.localtime(epoch))

    
if __name__ == '__main__':
    MinMaxCount.run()
