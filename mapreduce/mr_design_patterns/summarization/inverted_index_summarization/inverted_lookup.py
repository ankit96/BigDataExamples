from mrjob.job import MRJob
from mrjob.step import MRStep
import  time
from mrjob.protocol import RawValueProtocol,RawProtocol
import json


class inverted_lookup(MRJob):
    
    OUTPUT_PROTOCOL = RawValueProtocol
    HADOOP_OUTPUT_FORMAT = 'nicknack.MultipleValueOutputFormat'
    LIBJARS = ['nicknack-1.0.0.jar']

    def __init__(self, *args, **kwargs):
        super(inverted_lookup, self).__init__(*args, **kwargs)
       
    #defining steps 
    def steps(self):    
        return [
            MRStep(mapper=self.mapper,
                 reducer=self.reducer
                  )

        ]

    #MapReduce Phase 1 : convert temperature data into city,day,temp,temp_count
    def mapper(self, _, line):
        (city,temp,timestamp) = line.split('|')
        yield temp,(city,self.get_day_from_timestamp(timestamp))
       
    def reducer(self, temp , city_day):

        for data in city_day:
            #save city  date in directory named temp tab seperated
            yield None,'\t'.join([temp,json.dumps(data[0]),json.dumps(data[1])])

            
    ###################################################################################
    ## utility functions
    def get_day_from_timestamp(self,timestamp):
        one_day = 60*60*24
        return self.convert_epoch(int((int(timestamp)//one_day)*one_day))

    def convert_epoch(self,epoch):
        return time.strftime('%Y-%m-%d', time.localtime(epoch))
         

if __name__ == '__main__':
    inverted_lookup.run()


# command : python inverted_lookup.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar city_temperature.txt --output-dir hdfs:///user/maria_dev/inverted_lookup_out



# output :

# [root@sandbox mapreduce_examples]# hadoop fs -ls /user/maria_dev/inverted_lookup_out
# Found 37 items
# drwxr-xr-x   - root hdfs          0 2019-03-10 08:49 /user/maria_dev/inverted_lookup_out/10
# drwxr-xr-x   - root hdfs          0 2019-03-10 08:49 /user/maria_dev/inverted_lookup_out/11
# drwxr-xr-x   - root hdfs          0 2019-03-10 08:49 /user/maria_dev/inverted_lookup_out/12
#...
# -rw-r--r--   1 root hdfs          0 2019-03-10 08:49 /user/maria_dev/inverted_lookup_out/_SUCCESS



# [root@sandbox mapreduce_examples]# hadoop fs -cat /user/maria_dev/inverted_lookup_out/36/part-00000
# "Mumbai"    "2019-02-19"
# "Chennai"   "2019-02-19"
# .....
# "Chennai"   "2019-02-18"
# "Delhi" "2019-02-15"
# "Delhi" "2019-02-01"
# "Chennai"   "2019-02-26"

