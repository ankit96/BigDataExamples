from mrjob.job import MRJob
from mrjob.step import MRStep
import json


class counters(MRJob):
    
    
    def __init__(self, *args, **kwargs):
        super(inverted_lookup, self).__init__(*args, **kwargs)
       
    #defining steps 
    def steps(self):    
        return [
            MRStep(mapper=self.mapper
                  )

        ]

    #MapReduce Phase 1 : convert temperature data into city,day,temp,temp_count
    def mapper(self, _, line):
        (city,temp,timestamp) = line.split('|')
        self.increment_counter('temperature',temp,1)
        yield _,line
       
  

if __name__ == '__main__':
    counters.run()


# command : python counters.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar city_temperature.txt



# output :

# Counters: 66
#     File Input Format Counters 
#         Bytes Read=33143
#     File Output Format Counters 
#         Bytes Written=0
#     File System Counters
#         FILE: Number of bytes read=0
#         FILE: Number of bytes written=296614
#         FILE: Number of large read operations=0
#         FILE: Number of read operations=0
#         FILE: Number of write operations=0
#         HDFS: Number of bytes read=33507
#         HDFS: Number of bytes written=0
#         HDFS: Number of large read operations=0
#         HDFS: Number of read operations=10
#         HDFS: Number of write operations=4
#     Job Counters 
#         Data-local map tasks=2
#         Launched map tasks=2
#         Total megabyte-milliseconds taken by all map tasks=4368750
#         Total time spent by all map tasks (ms)=17475
#         Total time spent by all maps in occupied slots (ms)=17475
#         Total time spent by all reduces in occupied slots (ms)=0
#         Total vcore-milliseconds taken by all map tasks=17475
#     Map-Reduce Framework
#         CPU time spent (ms)=2900
#         Failed Shuffles=0
#         GC time elapsed (ms)=471
#         Input split bytes=364
#         Map input records=1000
#         Map output records=0
#         Merged Map outputs=0
#         Physical memory (bytes) snapshot=256909312
#         Spilled Records=0
#         Total committed heap usage (bytes)=88080384
#         Virtual memory (bytes) snapshot=3869114368
#     temperature
#         10=17
#         11=28
#         12=25
#         13=25
#         14=41
#         15=28
#         16=29
#         17=29
#         18=26
#         19=38
#         20=21
#         21=28
#         22=19
#         23=25
#         24=39
#         25=33
#         26=21
#         27=32
#         28=31
#         29=28
#         30=19
#         31=21
#         32=36
#         33=29
#         34=28
#         35=37
#         36=21
#         37=23
#         38=23
#         39=28
#         40=28
#         5=32
#         6=28
#         7=35
#         8=23
#         9=26
