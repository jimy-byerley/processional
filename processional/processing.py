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


__all__ = ['slave', 'server', 'client', 'serve', 'export',
			'SlaveProcess', 'RemoteObject']


# accept all child process exits
def handler(sig, stack):
	try:	os.wait()
	except ChildProcessError: pass
signal.signal(signal.SIGCHLD, handler)


def slave(address=None, main=None, detach=False) -> 'SlaveProcess':
	''' create a slave process connected to the current process with a `Pipe`
	
		Example:
			
			>>> process = slave()
			>>> process.invoke(lambda: print('hello from slave'))
		
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
	if not main:
		main = getattr(sys.modules['__main__'], '__file__', None)
	if main:
		if not isinstance(main, str):	 main = main.__file__
		if main:   args.extend(['-m', main])
	if detach:     args.append('-d')
	
	pid = os.spawnv(os.P_NOWAIT, sys.executable, args)
	
	slave = client(address or _default_address(pid))
	slave.pid = pid
	return slave
	
def server(address=None, main=None, persistent=False, detach=False, connect=True) -> 'SlaveProcess':
	''' create a server process that listen for any new connection and answer to any client command.
		The clients connect to that process using a socket (unix or inet socket)
		
		Example:
		
			>>> process = server('/tmp/server')
			>>> process.invoke(lambda: print('hello from server'))
			
		Args:
		
			address:
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
			
			connect:  
				if `True`, wait for the server process to start and return a `SlaveProcess` instance connected to it
			
		Note:
			a current limitation of this module is that subprocesses always become zombie at their exit because `os.wait` is never called since spawned subprocesses might continue to run
	'''
	args = [sys.executable, '-m', 'processional']
	if address:    args.extend(['-a', address])
	if not main:
		main = getattr(sys.modules['__main__'], '__file__', None)
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
			>>> process = client('/tmp/server')
			>>> process.invoke(lambda: print('hello from server'))
	
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
	slave = SlaveProcess(SocketConnection(client, timeout))
	slave.address = address
	return slave

def serve(address=None, main=None, persistent=False, detach=False, connect=True) -> 'SlaveProcess':
	''' like `server()` but the slave main loop is running in the current process in a dedicated thread. 
		
		this is useful to make the current thread a server and pass its address to an other process 
	'''
	if address and guess_socket_familly(address) == socket.AF_UNIX:
		try:	os.unlink(address)
		except FileNotFoundError: pass
	
	if not address:
		address = _default_address(os.getpid())
	
	thread = thread(host.Host(address, main, persistent=persistent, attached=not detach).server)
	if connect:
		slave = client(address)
		slave.thread = thread
		slave.pid = os.getpid()
		return slave
	else:
		return thread

def export(obj) -> 'RemoteObject':
	''' wrap an object in the local process.
	
		this is perfectly useless, except for passing its reference to an other process
	'''
	if previous := host.wrapped.get(id(obj)):
		previous.count += 1
	else:
		host.wrapped[id(obj)] = host.Wrapped(obj, 1)
	return RemoteObject(LocalWrappedObject(id(obj), True), (('item', id(obj)),))


class SlaveProcess:
	''' child process control object
	
		End users should get instances using `slave()`, `client()`, `server()` or so
		
		This class is thread-safe
		
		Attributes:
			sid:     the value of `processional.host.sid` in the remote process
			address: the address used to connect to this slave, or `None`
			pid:     the process pid (if this slave has been created as a subprocess, else `None`)
			instances:  a dictionnary of all living instances of `SlaveProcess`
	
		Example:
		
			>>> process = slave()
			
			Functions, closures and their cell variables are serialized and passed to the remote process, the result or error is retreived
			
			>>> cell = 'coucou'
			>>> process.invoke(lambda: print('value:', cell))  # cell variable
			>>> process.invoke(lambda cell=cell: print('value:', cell))  # binding through default argument
			>>> process.invoke(partial(print, 'value:', cell))  # binding
			
			>>> task = process.schedule(lambda: print('value:', cell))  # does not block
			>>> task.wait()
			
			Exceptions are propagated, but traceback is not passed between processes
			
			>>> thread = slave()
			>>> @thread.schedule
			... def root():
			... 	from math import sqrt
			... 	return sqrt(-1)
			>>> root.wait()
			Traceback (most recent call last):
			File ".../test.py", line 9, in <module>
				root.wait()
			File ".../processional/processional/processing.py", line 428, in wait
				raise err
			ValueError: math domain error
			
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
			
			This method is useful if you set the slave `persistent` and need to stop its server loop
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
		return RemoteObject(RemoteWrappedObject(self, remote, True), ((host.ITEM, remote),))
		
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
			
			If `timeout` is non-null, this function will wait only for the given time in seconds. If it is `None` it will wait indefinitely
		'''
		if timeout is None or self.connection.poll(timeout):
			id, err, result, report = self.connection.recv()
			# register for attending task
			if id in self.register:
				# notes can be added since python 3.11
				if err:
					try:    err.add_note('traceback on slave side:\n'+report)
					except NameError: pass
				self.register[id] = (err, result, report)
				with self.recvsig:
					self.recvsig.notify_all()
			# or report immediately if not attended
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

class NonSlave:
	''' mimic the behavior of a slave but execute everything in the current thread '''
	def invoke(self, func):	func()
	def schedule(self, func): func()
	@property
	def sid(self):	return host.sid

class RemoteWrappedObject(object):
	''' own or borrows a reference to a wrapped object on a slave 
	
		This object is just a data holder, the user should use `RemoteObject` instead
	'''
	__slots__ = 'slave', 'id', 'owned'
	
	def __init__(self, slave, id, owned):
		self.slave = slave
		self.id = id
		self.owned = owned
	
	def __del__(self):
		if self.owned:
			self.owned = False
			try:	self.slave.Task(self.slave, host.DROP, self.id)
			except OSError: pass
		
	def own(self):
		if not self.owned:
			self.owned = True
			self.slave.Task(self.slave, host.OWN, self.id)
	
class LocalWrappedObject(object):
	''' owns or borrows a reference to a wrapped object in the current process 
	
		This object is just a data holder, the user should use `RemoteObject` instead
	'''
	__slots__ = 'id', 'owned'
	slave = NonSlave()
	
	def __init__(self, id, owned):
		self.id = id
		self.owned = owned
	
	def __del__(self):
		if self.owned:
			self.owned = False
			host.wrapped[self.id].count -= 1
	
	def own(self):
		if not self.owned:
			self.owned = True
			host.wrapped[self.id].count += 1

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
		return '<{} {} in sid {} at {}>'.format(
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
			return self(RemoteWrappedObject(slave, address[0][1], False), address)
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
		''' ensure this process own a reference to the remote object 
		
			this method is not thread-safe
		'''
		return self._ref.own()
	
	def unwrap(self):
		''' retreive the referenced object in the current process. It must be pickleable '''
		return self.slave.invoke((host.unwrap, self._address))



def _format_address(address):
	it = iter(address)
	address = hex(next(it)[1])
	for kind, sub in it:
		if kind == host.ATTR:
			address += '.'
			address += str(sub)
		elif kind == host.ITEM:
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



