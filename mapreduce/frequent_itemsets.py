#problem statemnt credits : https://www.cdac.in/index.aspx?id=ev_hpc_frequent-itemset-mr1
from mrjob.job import MRJob
from mrjob.step import MRStep

import itertools

class frequent_itemsets(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.tuple_mapper,
                   reducer=self.tuple_reducer)
        ]

    def tuple_mapper(self, _, line):
        elements = line.split(' ')
        

        for L in range(0, len(elements)+1):
            for subset in itertools.combinations(elements, L):
                if len(subset)>0:
                    #print subset,1
                    yield subset,1

    def tuple_reducer(self, subset, count_list):
        total_count = sum(count_list)

        if total_count>=2:# here 2 = threshold for frequent itemsets
            print subset,total_count
	


if __name__ == '__main__':
    frequent_itemsets.run()


#python frequent_itemsets.py ../resources/item_sets.txt
