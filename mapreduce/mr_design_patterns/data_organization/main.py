from stuctured_to_hierarchical import Structured_to_Hierarical
import os


mr_job = Structured_to_Hierarical()
with mr_job.make_runner() as runner:
	runner._input_paths = ['/home/ankit/bigdata/big_data_problems/resources/city_temperature.txt','/home/ankit/bigdata/big_data_problems/resources/cities.txt']
	runner.run()
	for key, value in mr_job.parse_output(runner.cat_output()):
		print(key,value)