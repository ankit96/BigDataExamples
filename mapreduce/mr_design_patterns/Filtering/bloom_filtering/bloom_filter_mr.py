from mrjob.job import MRJob
from mrjob.step import MRStep
from bloomfilter import BloomFilter 


class BloomFilterMR(MRJob):
    
    
    def __init__(self, *args, **kwargs):
        super(BloomFilterMR, self).__init__(*args, **kwargs)
        self.n = 20
        self.p = 0.05
        self.hot_list = [1,8,14,12,23,31,55]
		
    

    #defining steps 
    def steps(self):    
        return [
            MRStep(mapper_init=self.mapper_init
            		,mapper=self.mapper
                  )

        ]

    def mapper_init(self):
		self.bloomf = BloomFilter(self.n,self.p) 

		for elem in self.hot_list:
			self.bloomf.add(str(elem))


    #MapReduce Phase 1 : convert temperature data into city,day,temp,temp_count
    def mapper(self, _, line):
        (city,temp,timestamp) = line.split('|')
        if self.bloomf.check(temp):
            yield city,(temp,timestamp)


if __name__ == '__main__':
    BloomFilterMR.run()


# command : python bloom_filter_mr.py  ../../../../resources/city_temperature.txt

