from mrjob.job import MRJob
from mrjob.step import MRStep

class word_count(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.word_mapper,
		          combiner = self.word_combiner,
                  reducer=self.word_reducer)
        ]

    def word_mapper(self, _, line):
        words = line.split(' ')
    	for word in words:
    	    if len(word)>0:
                yield word, 1

    def word_combiner(self, word, counts):
        yield (word, sum(counts))

    def word_reducer(self, word, counts):
        yield (word, sum(counts))


if __name__ == '__main__':
    word_count.run()


# command : python word_count.py ../resources/example_text.txt
