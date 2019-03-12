from distributed_grep import DistributedGrep
import os


mr_job = DistributedGrep()
with mr_job.make_runner() as runner:
	runner._input_paths = ['/home/ankit/bigdata/big_data_problems/resources/city_temperature.txt']
	runner.run()
	for key, value in mr_job.parse_output(runner.cat_output()):
		print(key,value)