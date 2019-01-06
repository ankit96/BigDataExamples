from mrjob.job import MRJob
from mrjob.step import MRStep

class sorted_word_count(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.word_mapper,
		          combiner = self.word_combiner,
                   reducer=self.word_reducer),
            MRStep(reducer=self.final_reducer)#optional for sorting
        ]

    def word_mapper(self, _, line):
        words = line.split(' ')
    	for word in words:
    	    if len(word)>0:
                yield word, 1

    def word_combiner(self, word, counts):
        yield (word, sum(counts))

    def word_reducer(self, word, counts):
        yield sum(counts),word


    def final_reducer(self, count,words):
        words_sorted = sorted(list(words),reverse=True)
        word_list='\t'.join(str(word) for word in words_sorted)
        print count,str(word_list)
	


if __name__ == '__main__':
    sorted_word_count.run()


#python sorted_word_count.py ../resources/example_text.txt
