#problem statemnt credits : https://www.cdac.in/index.aspx?id=ev_hpc_hadoop-map-reduce#hadoop-map-reduce-par-prog-id11
from mrjob.job import MRJob
from mrjob.step import MRStep



class matrix_multiplication(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.tuple_mapper,
                   reducer=self.tuple_reducer)
        ]

    def tuple_mapper(self, _, line):
        elements = line.split('->')
        matrix = elements[0].strip()
        element = elements[1].strip().split(' ')
        i = element[0].strip()
        j = element[1].strip()
        value = element[2].strip()

        if matrix=='A':
        	for k in range(1,4):
        		yield ((i,str(k)),(matrix,j,value))
        else:
        	for k in range(1,3):
        		yield ((str(k),j),(matrix,i,value))


    def tuple_reducer(self, index, values):


		hashA =  {}
		hashB =  {}

		for (x, j, a_ij) in values:
			if x == 'A':
				hashA[j] = a_ij
			else:
				hashB[j] = a_ij

		result = 0
		for j in hashA.keys():
			result = result +  (int(hashA[j]) * int(hashB[j]))
		print index,result
	


if __name__ == '__main__':
    matrix_multiplication.run()


#python matrix_multiplication.py ../resources/matrix_test_case.txt
