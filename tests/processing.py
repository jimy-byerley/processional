from processional.processing import *
from time import sleep
from functools import partial
import sys, os, traceback


def pid_exists(pid):
	try:	os.kill(pid, 0)
	except ProcessLookupError:  return False
	else:	return True

def test_slaveprocess():
	process = slave()

	# one only master
	try:	client(process.address, timeout=0.1)
	except (TimeoutError, ValueError):  pass
	else:   assert False
	
	# task scheduling
	task = process.schedule(lambda: 'ok')
	assert task.wait() == 'ok'
	# task invocation
	assert process.invoke(lambda: 'ok') == 'ok'
	cell = 'coucou'
	assert process.invoke(lambda: ('value', cell)) == ('value', cell)

	# function restoring
	# functions from modules
	from time import sleep
	assert process.invoke(lambda: sleep.__module__ == 'time')
	process.invoke(lambda: sleep(1e-3))
	process.invoke(partial(sleep, 1e-3))
	# local functions, eventually nested
	def local_function():
		return True
	assert process.invoke(lambda: local_function())
	assert process.invoke(local_function)
	
	# cell overriding global variables
	min = lambda: True
	assert process.invoke(lambda: min())
	
	# module importation
	import sys
	from pnprint import nprint  # a module not automatically imported by processional slaves
	assert process.invoke(lambda: 'pnprint' not in sys.modules)
	process.invoke(lambda: nprint)
	assert process.invoke(lambda: 'pnprint' in sys.modules)
	
	# threaded invocations
	job = process.thread(lambda: sleep(0.2) or True)
	assert not job.complete()  # long task is running
	assert process.invoke(lambda: True)  # other task can run in background
	assert not job.complete()  # long task still not done
	assert job.wait()
	
	# sending large data
	for i in range(5):
		big = b'123'*10000
		received = process.invoke(lambda: big)
		assert big == received
		
	# error handling
	import traceback
	def subfunc():
		print(hello)
	def func():
		subfunc()
	# propagating exceptions
	task = process.schedule(func)
	try:
		task.wait()
	except Exception as err:
		# check that the stack has all layers
		print('caught exception:', file=sys.stderr)
		traceback.print_exception(err)
	else:
		assert False
	# check exceptions from dropped tasks
	print('dropped exception:', file=sys.stderr)
	process.schedule(func)
	process.poll(1)
	
def test_slaveprocess_closing():
	# slace exit on master disconnection
	process = slave()
	process.thread(lambda: sleep(10) or print('done'))
	pid = process.pid
	del process
	sleep(1e-1)
	assert not pid_exists(pid)
	
	# slave exit only after threads done if detached
	process = slave(detach=True)
	process.thread(lambda: sleep(1))
	pid = process.pid
	del process
	sleep(1e-1)
	assert pid_exists(pid)
	sleep(1.1)
	assert not pid_exists(pid), mid

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
	
def test_serverprocess_closing():
	# disconnection on drop and server exit on last client disconnection
	process = server()
	pid = process.pid
	del process
	sleep(1e-1)
	assert not pid_exists(pid), pid
	
	# server not exiting if persistent
	address = '/tmp/processional-test'
	pid = server(address, persistent=True, connect=False)
	client(address).close()
	sleep(1e-1)
	assert pid_exists(pid)
	client(address).stop()
	sleep(1e-1)
	assert not pid_exists(pid)
	
def test_remoteobject():
	process = server()
	# wrapping and unwrapping
	l = process.wrap(lambda: [1,2,3])
	assert l.unwrap() == [1,2,3]
	
	# method invocation
	assert l.__len__() == 3
	l.append(5)
	assert l.unwrap() == [1,2,3,5]
	# dereferencing while sending in its own thread
	i = process.invoke(lambda: id(l))
	assert process.invoke(lambda: id(l) == i)
	assert process.invoke(lambda: l == [1,2,3,5])
	
	# dropping
	@process.wrap
	def owned():
		class A: pass
		return A()
	@process.wrap
	def weak():
		import weakref
		return weakref.ref(owned)
	# a weak reference is held
	assert process.invoke(lambda: weak() is not None)
	# dropping the owning ref in the current process
	del owned
	# ... also released the object in the remote process
	assert process.invoke(lambda: weak() is None)
	
	# a second process to demonstrate remote objects shared between clients
	second = slave()
	# remote object cannot be transfered without explicit connection
	try:	second.invoke(lambda: l)
	except ValueError:  pass
	else:	assert False
	# remote object can be transfered while already connected
	hook = second.connect(process)
	assert second.invoke(lambda: isinstance(l, RemoteObject))
	assert second.invoke(lambda: l.unwrap() == [1,2,3,5])
	
	# export
	wrapped = process.invoke(lambda: export(l))
	assert isinstance(wrapped, RemoteObject)
	assert wrapped.unwrap() == [1,2,3,5]
	assert wrapped is not l
	assert process.invoke(lambda: wrapped is l)
	
	# dropping in case of disconnections
	second = client(process.address)
	a = process.wrap(lambda: 1)
	b = process.wrap(lambda: 2)
	sa = second.wrap(lambda: a)
	process.close()
	sa.unwrap()
	try:	second.wrap(lambda: b)
	except ReferenceError:  pass
	else:   assert False

