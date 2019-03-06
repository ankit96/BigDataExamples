from mrjob.job import MRJob
from mrjob.step import MRStep
import  time


class median(MRJob):
    
    def __init__(self, *args, **kwargs):
        super(median, self).__init__(*args, **kwargs)

    #defining steps 
    def steps(self):    
        return [
            MRStep(mapper=self.number_mapper,
		          combiner = self.number_combiner,
                  reducer=self.number_frequency_reducer),
            MRStep(reducer=self.sorting_reducer)

        ]

    #MapReduce Phase 1 : onvert temperature data into city,day,temp,temp_count
    def number_mapper(self, _, line):
        (city,temp,timestamp) = line.split('|')
        yield (city,self.get_day_from_timestamp(timestamp),int(temp)), 1
    
    def number_combiner(self, city_day_temp, count):
        yield city_day_temp,sum(count)

    def number_frequency_reducer(self, city_day_temp, count):
        yield (city_day_temp[0],city_day_temp[1]),(city_day_temp[2],sum(count))
        
    #MapReduce Phase 2
    def sorting_reducer(self, city_day, day_stats):

        day_stats = list(day_stats) #safe to do since distinct temperature in a city for a single day is well inbound
        day_stats = sorted(day_stats, key=lambda day_stats: day_stats[0])

        total_records = sum(x[1] for x in day_stats)
        
        median_index = total_records/2 + 1
        median_index2 = median_index

        if total_records%2==0 : 
            median_index -= 1
        

        csum = 0
        old_csum = 0
        result_set = []

        for data in day_stats:
            #print csum,old_csum,prev_key
            csum += data[1]
            if  csum >= median_index  and old_csum<median_index2:
                result_set.append(data[0])

            old_csum = csum
            prev_key = city_day[1]  
        #print(result_set)  
        print(city_day[0],self.convert_epoch(city_day[1]),sum(result_set)/(1.0*len(result_set)))
        
            
    ###################################################################################
    ## utility functions
    def get_day_from_timestamp(self,timestamp):
        one_day = 60*60*24
        return int((int(timestamp)//one_day)*one_day)

    def convert_epoch(self,epoch):
        return time.strftime('%Y-%m-%d', time.localtime(epoch))
         

if __name__ == '__main__':
    median.run()


# command : python median.py ../../../resources/city_temperature.txt 

