from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq 

class mheap(): 

    def __init__(self, topx): 
        self.limit = topx
        self.heap = []

    def push(self,value):

        if len(self.heap) < self.limit:
            heapq.heappush(self.heap, value)
        else:
            spilled_value = heapq.heappushpop(self.heap, value)




class Top10(MRJob):

    #defining steps 
    def steps(self):    
        return [
            MRStep(mapper_init=self.mapper_init
            		,mapper=self.mapper
                    ,mapper_final=self.mapper_final
                    ,reducer_init=self.reducer_init
                    ,reducer=self.reducer
                    ,reducer_final=self.reducer_final
                  )

        ]

    def mapper_init(self):
        self.mapper_heap = mheap(10)


    def mapper(self, _, line):
        (user_id,reputation) = line.split(',')
        self.mapper_heap.push((reputation,user_id))


    def mapper_final(self):
        
        top10localrepu = self.mapper_heap.heap
        for rep in top10localrepu:
            yield None,rep


    def reducer_init(self):
        self.reducer_heap = mheap(10)


    def reducer(self, _, top_reputations):
        for repu in top_reputations:
            self.reducer_heap.push(repu)

    def reducer_final(self):
        
        top10globalrepu = self.reducer_heap.heap
        for rep in sorted(top10globalrepu,reverse = True):
            yield rep


    
if __name__ == '__main__':
    Top10.run()


# command : python top10.py ../../../resources/user_reputation.csv

