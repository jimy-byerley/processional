from processional import *

def test_serverprocess():
	# the implementation of servers and slaves are mostly the same code, so we will test only server-specific features here
	
	# instantiation and connection
	process = server('/tmp/processional-test')
	second = client(process.address)
	# so many clients
	others = [client(process.address)  for i in range(100)]
	# disconnections
	del others
	
	# tasks executed in the scheduling order guaranteed for one client
	from time import sleep, time
	from functools import reduce
	first_tasks = [
		process.schedule(lambda: sleep(1e-2) or time())
		for i in range(100)]
	second_tasks = [
		second.schedule(lambda: sleep(1e-2) or time())
		for i in range(100)]
	assert reduce(lambda a, b: a < b, (task.wait() for task in first_tasks))
	assert reduce(lambda a, b: a < b, (task.wait() for task in second_tasks))
	
if __name__ == "__main__":
	test_serverprocess()