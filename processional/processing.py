from . import host
from .connection import SocketConnection, guess_socket_familly
from .shared import Diller, Pickler

import sys, traceback
import os, socket, signal
from operator import setitem, getitem
from functools import partial
from math import inf
from time import sleep, time
from weakref import WeakValueDictionary
from threading import Lock, Condition
from io import BytesIO as StringIO


__all__ = ['slave', 'server', 'client', 'localserver', 
			# 'localwrap', 
			'SlaveProcess', 'RemoteObject']


# accept all child process exits
signal.signal(signal.SIGCHLD, lambda sig, stack: os.wait())


def slave(address=None, main=None, detach=False) -> 'SlaveProcess':
	''' create a slave process connected to the current process with a `Pipe`
		
		Arguments:
			
			address:	
			
				the address to use to communicate with the slave, if ommited an address is automatically picked
				- it can be a network address as a tuple `(ip:str, port:int)` with `ip` being an address to the local computer on the target network
				- on linux it can be the file name of a Unix socket to create
			
			main:  
				the name of a python module to use as __main__ module, or a path to a python file to execute as __main__ when initializing the server.
				if ommited, an empty module is created
				
			detach:     
			
				if true, the slave will be considered detached an will survive the disconnection of the current process
				it is equivalent to calling `SlaveProcess.detach()` later
	'''
	args = [sys.executable, '-m', 'processional', '-s']
	if address:    args.extend(['-a', address])
	if main:
		if not isinstance(main, str):	 main = main.__file__
		if main:   args.extend(['-m', main])
	if detach:     args.append('-d')
	
	pid = os.spawnv(os.P_NOWAIT, sys.executable, args)
	
	slave = client(address or _default_address(pid))
	slave.pid = pid
	return slave
	
def server(address=None, main=None, persistent: callable=False, detach=False, connect=True) -> 'SlaveProcess':
	''' create a server process that listen for any new connection and answer to any client command.
		The clients connect to that process using a socket (unix or inet socket)
		
		Example:
		
			>>> client1 =: callable server('/tmp/server')
			>>> o = client1.wrap(lambda: 'hello message')
			
			>>> client2 = client('/tmp/server')
			>>> client2.invoke(lambda: print(o)): callable
			hello message
		
		Args:
		
			address:  
				: callable
				the server address
				- it can be a network address as a tuple `(ip:str, port:int)` with `ip` being an address to the local computer on the target network
				- on linux it can be the file name of a Unix socket to create
			
			main:  
				the name of a python module to use as __main__ module, or a path to a python file to execute as __main__ when initializing the server.
				if ommited, an empty module is created
				
			detach:     
				if true, the slave will be considered detached an will survive the disconnection of the current process
				it is equivalent to calling `SlaveProcess.detach()` later
				
			persistent:  
				if `True` the server stay active when no clients is connected. Else it automatically stops its main loop and finish its threads.
				It is equivalent to calling `SlaveProcess.persist()` later
			
			connect:  if `True`, wait for the server process to start and return a `SlaveProcess` instance connected to it
			
		Note:
			a current limitation of this module is that subprocesses always become zombie at their exit because `os.wait` is never called since spawned subprocesses might continue to run
	'''
	args = [sys.executable, '-m', 'processional']
	if address:    args.extend(['-a', address])
	if main:
		if not isinstance(main, str):	 main = main.__file__
		if main:   args.extend(['-m', main])
	if detach:     args.append('-d')
	if persistent: args.append('-p')
	
	if address and guess_socket_familly(address) == socket.AF_UNIX:
		try:	os.unlink(address)
		except FileNotFoundError: pass
	
	pid = os.spawnv(os.P_NOWAIT, sys.executable, args)
	
	if connect:
		slave = client(address or _default_address(pid))
		slave.pid = pid
		return slave
	else:
		return pid
		
def client(address, timeout:float=None) -> 'SlaveProcess':
	''' create a `SlaveProcess` instance connected to an already existing server process 
	
		Example:
		
			>>> server('/tmp/server', connect=False)
			>>> slave = client('/tmp/server')
			>>> slave.invoke(lambda: print('hello from slave'))
	
		Args:
			address:  
				
				the server address
				- it can be a network address as a `tuple` `(ip:str, port:int)` with `ip` being an address to the local computer on the target network
				- on linux it can be the file name of a Unix socket to create
				
			timeout:  time (seconds) allowed for connecting to the server. None for infinity
	'''
	family = guess_socket_familly(address)
	
	# in case of a unix socket, the connection expects an existing file so no timeout is applied in socket.connect
	# this will reproduce one
	if family == socket.AF_UNIX:
		if timeout is not None:
			timeoff = time()+timeout
		while not os.path.exists(address):
			if timeout is not None and time() > timeoff:
				raise TimeoutError('server not found')
			sleep(0.01)
	
	client = socket.socket(family, socket.SOCK_STREAM)
	client.settimeout(timeout)
	client.connect(address)
	slave = SlaveProcess(SocketConnection(client))
	slave.address = address
	return slave

def localserver(address, persistent=False, connect=True) -> 'SlaveProcess':
	''' like `server()` but the slave main loop is running in the current process in a dedicated thread. 
		this is useful to make the current thread a server and pass its address to an other process 
	'''
	thread = spawn(lambda: server_main(address, None, persistent))
	if connect:
		slave = client(address)
		slave.thread = thread
		return slave

# def localwrap(value) -> 'LocalObject':
# 	''' wrap an object in the local process.
# 		this is perfectly useless, except for passing its reference to an other process
# 	'''
# 	slave_env[id(value)] = value
# 	return LocalObject((('item', id(value)),))


class SlaveProcess:
	''' child process control object
	
		End users should get instances using `slave()`, `client()`, `server()` or so
		
		This class is thread-safe
		
		Args:
			connection:  The connection object to the slave. Not for end user
			register:  if True, this `SlaveProcess` instance will be registered as implicit bridge to the remote sid. Not for end user
	
		Example:
		
			>>> process = slave()
			
			>>> cell = 'coucou'
			>>> process.invoke(lambda: print('value:', cell))  # the closure is serialized with its variables and passed to the remote process
			
			>>> task = process.schedule(lambda: print('value:', cell))  # this variant split the closure send from the task awaiting
			>>> task.wait()
			
		Note:
		
			About performance, the serialization of lambda functions or any complex data structure (not necessarily big) can be slow (20 ms). For minimum delay it is better to use whenever possible `functools.partial` instead of a plain `lambda` function. the `partial` should wrap a function existing in a module on the salve side
			
			For fast interactions between slave and master using simple commands (like calling the methods of an object on the slave side), using a wrapped `RemoteObject` can be much faster than sending lambdas
	'''
	instances = WeakValueDictionary()
	max_unpolled = 200
			
	def __init__(self, connection=None, register=True):
		self.pid = None
		self.address = None
		
		self.id = 0		# max task id used
		self.unpolled = 0   # number of unpolled 
		self.register = {}	# tasks results indexed by task id
		self.sendlock = Lock()
		self.recvlock = Lock()	# locks access to the pipe
		self.recvsig = Condition()	# signal emitted when a result has been received
		self.connection = None
		
		self.sid = connection.recv()   # slave id of the remote process
		self.connection = connection
		
		if register:
			self.instances[self.sid] = self
			
	def __del__(self):
		self.close()
	
	def __repr__(self):
		return '<{} {}>'.format(type(self).__name__, self.sid)
		
	def __reduce__(self):
		return self._restore, (self.sid,)
		
	@classmethod
	def _restore(self, sid):
		slave = self.instances.get(sid)
		if slave is None:
			raise NameError('there is no active connection to {} in this process'.format(sid))
		return slave
		
	def terminate(self):
		''' kill the slave if it is a subprocess, else raise `ValueError`
			
			This method is provided in case it is needed to make sure a slave stops, but it is generally better to keep slaves attached and simply disconnect letting them deinitialize everything
		'''
		if self.pid:
			os.kill(self.pid, signal.SIGTERM)
		else:
			raise ValueError('this slave is not process on this machine')
		
	def stop(self) -> 'Task':
		''' stop the slave. any command sent after this will be ignored 
		
			The server process might continue to run threads
			
			This method useful if you set the slave `persistent` and need to stop its server loop
		'''
		return self.Task(self, host.CLOSE, None)
		
	def close(self):
		''' close the connection to the child process
		
			the server might continue to live to run tasks or threads or serve other clients
		'''
		if self.connection:
			try:	self.connection.close()
			except OSError: pass
	
	def detach(self) -> 'Task':
		''' set the child process to not exit when the master/last client disconnects 
		
			while not detached (default) a slave/server will end all its threads when its master/all its clients disconnected
		'''
		return self.Task(self, host.DETACH, None)
		
	def persist(self) -> 'Task':
		''' set the server process to not stop waiting new connections when the last client disconnects 
		
			while not persistent, a server stops its reception loop when it has no more clients
		'''
		return self.Task(self, host.PERSIST, None)
	
	def schedule(self, func: callable) -> 'Task':
		''' schedule a blocking task for the child process, 
		    
		    return a Thread proxy object that allows to wait for the call termination and to retreive the result.
		'''
		return self.Task(self, host.BLOCK, func)
		
	def invoke(self, func: callable):
		''' schedule a blocking task for the child process, and wait for its result.
			
			The result is retreived and returned.
			
			This is a shorthand to `self.schedule(func).wait()`
		'''
		return self.schedule(func).wait()
		
	def thread(self, func: callable) -> 'Task':
		''' schedule a threaded task on the child process and return a Thread proxy object to wait for the result.
			
			the call is executed in a thread on the child process, so other calls can be started before that one's termination.
		'''
		return self.Task(self, host.THREAD, func)
	
	def wrap(self, func: callable) -> 'RemoteObject':
		''' return a proxy on an object living in the child process 
			
			The proxy object tries behaves as the object living in the child process by sending every method call it the process.
		'''
		remote = self.Task(self, host.WRAP, func).wait()
		return RemoteObject(WrappedObject(self, remote, True), ((host.ITEM, remote),))
		
	def connect(self, process) -> 'RemoteObject':
		''' connect the slave to a given server process or address
			
			The slave will remain connected to the server as long as you do not drop the returned RemoteObject or the slave owns references to this connection
			
			This function is simply a shorthand to `self.wrap(partial(client, process.address))`
		'''
		if isinstance(process, SlaveProcess):
			process = process.address
		return self.wrap(partial(client, process))
	
	def poll(self, timeout:float=0):
		''' wait for reception of any result, return True if some are ready for reception 
			
			If `timeout` is non-null or None, poll will wait for reception
		'''
		if timeout is None or self.connection.poll(timeout):
			id, err, result, report = self.connection.recv()
			if id in self.register:
				self.register[id] = (err, result, report)
				with self.recvsig:
					self.recvsig.notify_all()
			elif err:
				print('Exception in', self, file=sys.stderr)
				print(report, file=sys.stderr)
			return True
		return False
				
	def _unpoll(self):
		self.unpolled += 1
		if self.unpolled > self.max_unpolled and self.recvlock.acquire(False):
			try:
				while self.poll(0):	pass
				self.unpolled = 0
			finally:
				self.recvlock.release()
					

	class Task(object):
		''' task awaiter '''
		__slots__ = 'slave', 'id', 'start'
		
		def __init__(self, slave, op, code):
			self.start = time()
			self.slave = slave
			with self.slave.sendlock:
				self.slave.id += 1
				self.id = self.slave.id
			
			if self.id not in self.slave.register:
				self.slave.register[self.id] = None
				
				if op in (host.BLOCK, host.WRAP, host.THREAD):
					if callable(code):
						file = StringIO()
						Diller(file).dump(code)
						code = file.getvalue()
					elif code and not isinstance(code, tuple):
						raise TypeError('code must be callable')
				
				self.slave._unpoll()
				
				with self.slave.sendlock:
					self.slave.connection.send((self.id, op, code))
			
		def __del__(self):
			termination = self.slave.register.pop(self.id, None)
			if termination:
				err, result, report = termination
				if err:
					print('Exception in', self, file=sys.stderr)
					print(report, file=sys.stderr)
			
		def __repr__(self):
			return '<{} {} on {}>'.format(type(self).__name__, self.id, self.slave.sid)
			
		def available(self) -> bool:
			if self.slave.register[self.id]:	return True
			if self.slave.recvlock.acquire(False):
				try:		self.slave.poll(0)
				finally:	self.slave.recvlock.release()
			if self.slave.register[self.id]:	return True
			return False
		
		def complete(self) -> bool:
			available = self.available()
			if available:
				if self.error:
					self.slave.register[self.id] = None
					raise self.error
			return available
		
		def wait(self, timeout:float=None):
			''' wait for the task termination and check for exceptions '''
			if not self.slave.register[self.id]:
				delay = timeout
				if timeout is not None:
					end = time()+timeout
				while True:
					# receive
					if self.slave.recvlock.acquire(False):
						try:		self.slave.poll(delay)
						finally:	self.slave.recvlock.release()
					else:
						with self.slave.recvsig:
							self.slave.recvsig.wait(delay)
					# check reception
					if self.slave.register[self.id]:
						break
						
					if timeout is None:		delay = None
					else:					delay = end-time()
					if timeout is not None and delay <= 0:
						raise TimeoutError('nothing received within allowed time')
			err, result, report = self.slave.register[self.id]
			self.slave.register[self.id] = None
			if err:
				raise err
			return result

		
class WrappedObject(object):
	''' own or borrows a reference to a wrapped object on a slave 
	
		But this object is just a data holder, the user should use `RemoteObject` instead
	'''
	__slots__ = 'slave', 'id', 'owned'
	
	def __init__(self, slave, id, owned):
		self.slave = slave
		self.id = id
		self.owned = owned
	
	def __del__(self):
		if self.owned:
			try:	self.slave.Task(self.slave, host.DROP, self.id)
			except OSError: pass
		
	def own(self):
		if not self.owned:
			Task(self.slave, host.OWN, self.id)
			self.owned = True
	
class RemoteObject(object):
	''' proxy object over an object living in a slave process 
		
		Its pickle interface is meant to send it in other processes
		
		- In the referenced object's owning process, it unpicles to the original object
		- In other processes, it unpickles to a `RemoteObject` communicating to the same owning process. the owning process must be a server and the destination process be already connected to that server through a `SlaveProcess`
		
		End user should get instances using `SlaveProcess.wrap()`
		
		Example:
			
			>>> o = process.wrap(lambda: [1,2,3])
			>>> o.append(5)
			>>> o
			<RemoteObject in sid=... at 0x...>
			>>> o.unwrap()
			[1,2,3,5]
	
		Attributes:
			slave (SlaveProcess):   the process owning the referenced object
	'''
	__slots__ = '_ref', '_address'
	
	def __init__(self, ref, address):
		# ref is the object owning or borrowing the reference on the slave
		self._ref = ref
		# address is the sub element of the referenced object
		self._address = address
			
	def __repr__(self):
		''' dummy implementation '''
		return '<{} {} in sid={} at {}>'.format(
			type(self).__name__, 
			'owned' if self._ref.owned else 'borrowed', 
			self._ref.slave.sid, 
			_format_address(self._address),
			)
		
	def __reduce__(self):
		''' the serialization only references the process object, it can only be reconstructed in the referenced process '''
		return RemoteObject._restore, (self._ref.slave.sid, self._address)
	
	@classmethod
	def _restore(self, sid, address):
		if sid == host.sid:
			return host.unwrap(address)
		slave = SlaveProcess.instances.get(sid)
		if slave:
			return self(WrappedObject(slave, address[0][1], False), address)
		raise ValueError('cannot represent {} from {} in {} out of its owning process and in unconnected process'.format(
					_format_address(address),
					sid,
					host.sid,
					))
	
	def __getitem__(self, key):
		''' create a speculative reference on an item of this object '''
		return RemoteObject(self._ref, (*self._address, (host.ITEM, key)))
	def __getattr__(self, key):
		''' create a speculative reference on an attribute of this object '''
		if key == 'slave':
			return self._ref.slave
		else:
			return RemoteObject(self._ref, (*self._address, (host.ATTR, key)))
	def __setitem__(self, key, value):
		''' send a value to be assigned to the referenced object item '''
		self.slave.schedule((setitem, self, key, value))
	def __setattr__(self, key, value):
		''' send a value to be assigned to the referenced object attribute '''
		if key in self.__slots__:
			super().__setattr__(key, value)
		else:
			self.slave.schedule((setattr, self, key, value))
	def __delitem__(self, key):
		''' send an order to delete the specified item in the referenced object '''
		self.slave.schedule((delitem, self, key))
	def __delattr_(self, key):
		''' send an order to delete the specified attribute in the referenced object '''
		if key in self.__slots__:
			super().__delattr__(key)
		else:
			self.slave.schedule((delattr, self, key))
	
	def __call__(self, *args, **kwargs):
		''' invoke the referenced object '''
		return self.slave.invoke((host.call, self._address, args, kwargs))
		
	def own(self):
		''' ensure this process own a reference to the remote object '''
		return self._ref.own()
	
	def unwrap(self):
		''' retreive the referenced object in the current process. It must be pickleable '''
		return self.slave.invoke((host.unwrap, self._address))

			

def _format_address(address):
	it = iter(address)
	address = hex(next(it)[1])
	for kind, sub in it:
		if kind == 'attr':	
			address += '.'
			address += str(sub)
		elif kind == 'item':
			address += '['
			address += repr(sub)
			address += ']'
		else:
			address += '<'
			address += repr(sub)
			address += '>'
	return address

def _default_address(pid):
	return '/tmp/process-{}'.format(pid)




def pid_exists(pid):
	try:	os.kill(pid, 0)
	except ProcessLookupError:  return False
	else:	return True

def test_slaveprocess():
	process = slave()

	# one only master
	try:	client(process.address, timeout=0.1)
	except TimeoutError:  pass
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
	
	# localwrap
	# TODO
	
	# dropping in case of diconnections
	# TODO
