from mrjob.job import MRJob
from mrjob.step import MRStep


class Distinct(MRJob):
           
    #defining steps 
    def steps(self):    
        return [
            MRStep(mapper=self.mapper
                    ,combiner= self.combiner
                    ,reducer= self.reducer
                  )

        ]

    def mapper(self, _, line):
        (city,temp,timestamp) = line.split('|')
        yield (city,temp),None

    def combiner(self,key,_):
        yield key,None

    def reducer(self,key,_):
        yield key,None


if __name__ == '__main__':
    Distinct.run()


# command : python distinct.py ../../../resources/city_temperature.txt

