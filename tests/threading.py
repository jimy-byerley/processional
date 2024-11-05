from processional.threading import *
import sys, os, traceback


def test_thread():
	from time import sleep
	# check the return value
	@thread
	def task():
		sleep(0.2)
		return 'hello'
	# check the task status while progressing
	assert not task.available()
	assert not task.complete()
	# check the returned value
	assert task.wait() == 'hello'
	assert task.wait() == 'hello'
	assert task.available()
	assert task.complete()
	
	# check exception raising
	def subfunc():
		print(hello)
	def func():
		subfunc()
	task = thread(func)
	try:
		task.wait()
	except Exception as err:
		# check that the stack has all layers
		assert len(list(traceback.walk_tb(err.__traceback__))) == 5
		print('caught exception:', file=sys.stderr)
		traceback.print_exception(err)
	else:
		assert False
	
	# check exceptions from dropped tasks
	print('dropped exception:', file=sys.stderr)
	thread(func)

def test_slavethread():
	slave = SlaveThread(current_thread())
	# cehck the returned value
	task = slave.schedule(lambda: 'hello')
	assert not task.available()
	assert not task.complete()
	slave.step()
	assert task.wait() == 'hello'
	
	# check exception raising
	def subfunc():
		print(hello)
	def func():
		subfunc()
	# propagating exceptions
	task = slave.schedule(func)
	slave.step()
	try:
		task.wait()
	except Exception as err:
		# check that the stack has all layers
		assert len(list(traceback.walk_tb(err.__traceback__))) == 5
		print('caught exception:', file=sys.stderr)
		traceback.print_exception(err)
	else:
		assert False

	# check exceptions from dropped tasks
	print('dropped exception:', file=sys.stderr)
	slave.schedule(func)
	slave.step()
