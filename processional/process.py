from . import host
from .connection import SocketConnection, guess_socket_familly
from .shared import Diller, Pickler

import sys
import os, socket
from operator import setitem, getitem
from math import inf
from time import sleep, time
from weakref import WeakValueDictionary
from threading import Lock, Condition
from io import BytesIO as StringIO


__all__ = ['slave', 'server', 'client', 'localserver', 'localwrap', 
			'SlaveProcess', 'RemoteObject']



def slave(address=None, module=None, detach=False) -> 'SlaveProcess':
	''' create a slave process connected to the current process with a `Pipe`
		`func` is a function run before the commands reception loop
	'''
	args = [sys.executable, '-m', 'processional', '-s']
	if address:    args.extend(['-a', address])
	if module:
		if isinstance(module, str):	 file = module
		else:                        file = module.__file__
		if file:   args.extend(['-m', file])
	if detach:     args.append('-d')
	
	pid = os.spawnv(os.P_NOWAIT, sys.executable, args)
	
	slave = client(address or _default_address(pid))
	slave.pid = pid
	return slave
	
def server(address=None, module=None, persistent=False, detach=False, connect=True) -> 'SlaveProcess':
	''' create a server process that listen for any new connection and answer to any client command.
		The clients connect to that process using a socket (unix or inet socket)
		
		Example:
		
			>>> client1 = server('/tmp/server')
			>>> o = client1.wrap(lambda: 'some')
			
			>>> client2 = client('/tmp/server')
			>>> client2.invoke(lambda: print(o))
			some
		
		Args:
		
			address:  
				
				the server address
				- it can be a network address as a tuple `(ip:str, port:int)` with `ip` being an address to the local computer on the target network
				- on linux it can be the file name of a Unix socket to create
				
			persistent:  if `True` the server stay active when no clients is connected. Else it automatically stops its main loop and finish its threads.
			
			connect:  if `True`, wait for the server process to start and return a `SlaveProcess` instance connected to it
	'''
	args = [sys.executable, '-m', 'processional']
	if address:    args.extend(['-a', address])
	if module:
		if isinstance(module, str):	 file = module
		else:                        file = module.__file__
		if file:   args.extend(['-m', file])
	if detach:     args.append('-d')
	if persistent: args.append('-p')
	
	if guess_socket_familly(address) == socket.AF_UNIX:
		try:	os.unlink(address)
		except FileNotFoundError: pass
	
	pid = os.spawnv(os.P_NOWAIT, sys.executable, args)
	
	if connect:
		slave = client(address or _default_address(pid))
		slave.pid = pid
		return slave
		
def client(address, timeout=None) -> 'SlaveProcess':
	''' create a `SlaveProcess` instance connected to an already existing server process 
	
		Example:
		
			>>> server(b'/tmp/server', connect=False)
			>>> slave = client(b'/tmp/server')
			>>> slave.invoke(lambda: print('coucou'))
	
		Args:
			address:  
				
				the server address
				- it can be a network address as a tuple `(ip:str, port:int)` with `ip` being an address to the local computer on the target network
				- on linux it can be the file name of a Unix socket to create
				
			timeout:  time allowed for connecting to the server. None for infinity
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

def localwrap(value) -> 'LocalObject':
	''' wrap an object in the local process.
		this is perfectly useless, except for passing its reference to an other process
	'''
	slave_env[id(value)] = value
	return LocalObject((('item', id(value)),))


class SlaveProcess:
	''' child process control object
	
		End users should get instances using `slave()`, `client()`, `server()` or so
		
		This class is thread-safe
		
		Args:
			connection (Pipe):  The connection object is responsible of the communication between processes, it must implement the interface of `multiprocessing.Pipe`.
			register:  if True, this `SlaveProcess` instance will be registered as implicit bridge to the remote sid
	
		Example:
		
			>>> process = slave()
			
			>>> cell = 'coucou'
			>>> process.invoke(lambda: print('value:', cell))  # the closure is serialized with its variables and passed to the remote process
			
			>>> task = process.schedule(lambda: print('value:', cell))  # this variant split the closure send from the task awaiting
			>>> task.wait()
			
		Note:
		
			About performance, the serialization of lambda functions or any complex data structure (not necessarily big) can be slow (20 ms). It is hence recommended when using a `SlaveProcess` instance to pass module functions, partials, and to send lambda functions only when this function is meant to execute during a a certain amount of time on the remote side.
			
			For fast interactions between slave and master using simple commands, using a wrapped `RemoteObject` can be much faster than sending lambdas
	'''
	instances = WeakValueDictionary()
	max_unpolled = 200
			
	def __init__(self, connection=None, register=True):
		self.id = 0		# max task id used
		self.unpolled = 0   # number of unpolled 
		self.register = {}	# tasks results indexed by task id
		self.sendlock = Lock()
		self.recvlock = Lock()	# locks access to the pipe
		self.recvsig = Condition()	# signal emitted when a result has been received
		
		self.sid = connection.recv()   # slave id of the remote process
		self.connection = connection
		
		if register:
			self.instances[self.sid] = self
		
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
		
	def close(self) -> 'ProcessTask':
		''' stop the child process. any command sent after this will be ignored '''
		return ProcessTask(self, host.CLOSE, None)
	
	def detach(self) -> 'ProcessTask':
		''' set the child process to not exit when the master/last client disconnects '''
		return ProcessTask(self, host.DETACH, None)
		
	def persist(self) -> 'ProcessTask':
		''' set the server process to not stop waiting new connections when the last client disconnects '''
		return ProcessTask(self, host.PERSIST, None)
	
	def schedule(self, code) -> 'ProcessTask':
		''' schedule a blocking task for the child process, 
		    return a Thread proxy object that allows to wait for the call termination and to retreive the result.
		'''
		return ProcessTask(self, host.BLOCK, code)
		
	def invoke(self, code):
		''' schedule a blocking task for the child process, and wait for its result.
			The result is retreived and returned.
		'''
		return ProcessTask(self, host.BLOCK, code).wait()
		
	def thread(self, code) -> 'ProcessTask':
		''' schedule a threaded task on the child process and return a Thread proxy object to wait for the result.
			the call is executed in a thread on the child process, so other calls can be started before that one's termination.
		'''
		return ProcessTask(self, host.THREAD, code)
	
	def wrap(self, code) -> 'RemoteObject':
		''' return a proxy on an object living in the child process 
			The proxy object tries behaves as the object living in the child process by sending every method call it the process.
			
			Warning:
				As the reference held by the RemoteObject can be shared across processes, it is
				stored persistently on the slave side. The user is responsible to call its
				`release()` method when the object is no more necessary so the slave can garbage
				collect it if no longer referenced on the slave side
		'''
		remote = ProcessTask(self, host.WRAP, code).wait()
		return RemoteObject(WrappedObject(self, remote, True), ((host.ITEM, remote),))
	
	def poll(self, timeout=0):
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
				print('Exception in', self)
				print(report)
				traceback.print_exception(err)
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
					

class ProcessTask(object):
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
				try:
					self.slave.connection.send((self.id, op, code))
				except BrokenPipeError:	
					if code is None:
						self.slave.register[self.id] = (None, None, None)
					else:
						raise
		
	def __del__(self):
		termination = self.slave.register.pop(self.id, None)
		if termination:
			err, result, report = termination
			if err:
				print('Exception in', self)
				print(report)
				traceback.print_exception(err)
		
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
	
	def wait(self, timeout=None):
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
			ProcessTask(self.slave, host.DROP, self.id)
		
	def own(self):
		if not self.owned:
			ProcessTask(self.slave, host.OWN, self.id)
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
			address (list):  the referenced variable address in the global scope
	'''
	__slots__ = '_ref', '_address'
	
	def __init__(self, ref, address):
		# ref is the object owning or borrowing the reference on the slave
		self._ref = ref
		# address is the sub element of the referenced object
		self._address = address
			
	def __repr__(self):
		''' dummy implementation '''
		return '<{} in sid={} at {}>'.format(type(self).__name__, self._ref.slave.sid, _format_address(self._address))
		
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
					
	# @property
	# def slave(self):
		# return self._ref.slave
		
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







def test_presence():
	print('present !')
			
def test_slaveprocess():
	# import a big ressource
	import numpy as np
	
	# main thread invocations and cell variables
	process = slave()
	process.invoke(lambda: print('ok'))
	cell = 'coucou'
	process.invoke(lambda: print('value:', cell))
	process.close()
	
	# cell overriding global variables 
	process = slave()
	@process.invoke
	def job():
		global a, b, sleep
		from time import sleep
		a = 5
		b = 5
	b = 0
	process.schedule(lambda: print('a', a))
	process.schedule(lambda: print('b', b))
	process.schedule(lambda: test_presence())
	
	# module importation
	process.schedule(lambda: print('np', 'np' in globals(), 'np' in vars()))
	
	# threaded invocations
	assert process.invoke(lambda: a) == 5
	job = process.thread(lambda: sleep(1))
	print('waiting')
	process.schedule(lambda: print('waiting'))
	job.wait()
	print('ok')
	
	# sending large data
	for i in range(5):
		big = b'123'*10000
		remote = process.wrap(lambda: big)
		received = remote.unwrap()
		assert big == received
	
	# closing
	process.close()
	print('\nall done')
	
def test_remoteobject():
	process = slave()
	@process.invoke
	def setup():
		global a, b, c
		a = 'coucou'
		b = [1,2,3]
		def c(): return 10
		print('process init', __name__, globals().keys())
		
	# wrapp a simple variable and retreive its value
	process.invoke(lambda: 
		print('process 1', __name__, globals().keys())
		)
	a = process.wrap('a')
	nprint('a', a.unwrap())
	
	b = process.wrap('b')
	nprint('b', b.unwrap())
	b.reverse()
	nprint('b', b.unwrap())
	nprint('b[0]', b[0].unwrap())
	nprint('b[0]', b.__getitem__(0))
	
	c = process.wrap('c')
	nprint('c()', c())
	
	d = process.wrap(lambda: [1])
	d.append(2)
	nprint('d', d, d.unwrap())
	
	# closing
	process.close()
	print('\nall done')

def test_serverprocess():
	# import a big ressource
	import numpy as np
	import mmap
	import multiprocessing.shared_memory
	import multiprocessing.managers
	
	# main thread invocations and cell variables
	process = server('/tmp/truc')
	process.invoke(lambda: print('ok'))
	
	# sending many small commands
	for j in range(10):
		for i in range(100):
			process.schedule(lambda: 1)
		process.poll()
	
	# sending large data
	for i in range(5):
		big = b'123'*10000
		remote = process.wrap(lambda: big)
		received = remote.unwrap()
		assert big == received
	
	# test of memory sharing
	size = 10_000_000
	big = b'\x01' * size
	process.invoke(lambda: print(len(big)))
	
	shared = multiprocessing.shared_memory.SharedMemory(create=True, size=size)
	array = np.asanyarray(shared.buf)
	array[:] = 1
	process.invoke(lambda: print(len(shared.buf)))
	
	# multi-client test
	second = slave()
	foreigner = process.wrap(lambda: [0, 0])
	
	import processional
	print('main', processional.host.sid)
	print('process', process.sid)
	print('second', second.sid)
	
	co = second.wrap(lambda a=process.address: client(a))
	print(1)
	@second.invoke
	def job():  foreigner[0] = 1
	print(2)
	assert foreigner[0].unwrap() == 1
	print(3)
	second.close()
	
	process.close()
