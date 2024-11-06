from .connection import SocketConnection, SerializationError, guess_socket_familly
from .threading import thread

import sys, traceback, warnings
import os, socket, select, signal
from dataclasses import dataclass
from collections import Counter

import dill



# id of this process as a slave, it will not change for the lifetime of the process
sid = (socket.gethostname(), os.getpid())

# slave and server operations
CLOSE = 0
BLOCK = 1
THREAD = 2
WRAP = 3
DROP = 4
OWN = 5
PERSIST = 6
DETACH = 7
# possible elements in wrapped objects addresses
ITEM = 0
ATTR = 1

# dictionnary of wrapped objects in this process, available for all instances of Host
wrapped = {}


class Host:
	''' server process implementation
	
		multiple can be run in a single process, but most of the time there is only one created at the process start 
	'''
	def __init__(self, address, module=None, persistent=False, attached=False):
		''' open and initialize the server '''
		self.address = address
		self.socket = None
		# allows the server to continue running after the last client disconnected
		self.persistent = persistent
		# stops the current process after the last client disconnected
		self.attached = attached
		# all sockets to listen
		self.sockets = []
		# clients sockets and their wrapped objects
		self.clients = {}
		# server named variables
		self.env = {}
		
		if isinstance(self.address, str):
			self.address = self.address.encode('utf-8')
		
		# open the server socket
		self.socket = socket.socket(guess_socket_familly(self.address), socket.SOCK_STREAM)
		self.socket.bind(self.address)
		
		if module:
			self.env = module.__dict__
	
	def __del__(self):
		if self.socket:
			self._unlink()
			self.socket.close()
	
	def server(self):
		''' server process listening loop '''
		# welcome requests of new connections
		self.socket.listen()
		self.sockets.append(self.socket)
		while True:
			# wait for an incomming request
			ready, _, _ = select.select(self.sockets, [], [])
			# welcome new connections
			for sock in ready:
				if sock is self.socket:
					self._accept()
			# check receved commands, ready is not used because new commands can arrive during this loop
			if not self._step():
				break
			
			if not self.clients and not self.persistent:
				self._unlink()
				if self.attached:
					os.kill(os.getpid(), signal.SIGTERM)
				break
	
	def slave(self):
		''' slave process listening loop '''
		# wait for the connection of the master
		self.socket.listen(1)
		self._accept()
		# delete socket entry as no longer needed
		self._unlink()
		
		while True:
			# wait for incomming commands
			ready, _, _ = select.select(self.sockets, [], [])
			# check receved commands, ready is not used because new commands can arrive during this loop
			if not self._step():
				break
			
			if not self.clients:
				# the master does not need the slave anymore
				if self.attached:
					os.kill(os.getpid(), signal.SIGTERM)
				break
	
	def _accept(self):
		''' accept a client connection request '''
		sock, source = self.socket.accept()
		connection = SocketConnection(sock)
		connection.send(sid)
		self.sockets.append(sock)
		self.clients[id(sock)] = Client(connection, Counter())
		
	def _unlink(self):
		''' delete the socket file if using UNIX socket '''
		self.socket.listen(0)
		if self.socket.family == socket.AF_UNIX:
			try:	os.remove(self.address)
			except FileNotFoundError: pass
					
	def _step(self):
		''' execute all already scheduled tasks
			This is meant to be called periodically by the server event loop
		'''
		while True:
			busy = False
			for sock in self.sockets:
				if sock is self.socket:	continue
				client = self.clients[id(sock)]
				
				try:
					if not client.connection.poll(0):
						continue
					busy = True
					tid, op, code = client.connection.recv()
				except (EOFError, ConnectionResetError):
					# other end dropped the pipe
					for oid, increment in client.wrapped.items():
						self._drop(client, oid, increment)
					del self.clients[id(sock)]
					self.sockets.remove(sock)
					continue
				
				# for operations on the server itself, a closure cannot be passed from the client to the server because nothing the client can send can reference the server object, therefore the client passes an operation specifier
				# op is an enum value telling what to do with the code or with the server
				if op == CLOSE:
					# other end requested slave exit
					try:	client.connection.send((tid, None, None))
					except BrokenPipeError:	pass
					return False
				elif op == THREAD:
					thread(lambda: self._task(client, tid, code, self._run), not self.attached)
				elif op == BLOCK:
					self._task(client, tid, code, self._run)
				elif op == WRAP:
					self._task(client, tid, code, self._wrap)
				elif op == DROP:
					self._drop(client, code)
				elif op == OWN:	
					self._own(client, code)
				elif op == PERSIST:
					self.persistent = True
					self.connection.send((tid, None, None, None))
				elif op == DETACH:
					self.attached = False
					self.connection.send((tid, None, None, None))
			
			if not busy:
				break
		return True
	
	def _task(self, client, tid, code, run):
		''' runs `run` and sends the result or error to the given client '''
		result = error = report = None
		try:
			result = run(client, code)
		except Exception as err:
			report = traceback.format_exc()
			error = err
		try:
			client.connection.send((tid, error, result, report))
		except BrokenPipeError:
			if error:
				warnings.warn('exception not reported because client disconnected')
		except SerializationError as err:
			report = traceback.format_exc()
			client.connection.send((tid, err.args[1], None, report))
	
	def _run(self, client, code):
		''' run the given code '''
		if isinstance(code, str):
			result = self.env[code]
		if isinstance(code, tuple):
			result = code[0](*code[1:])
		else:
			code = dill.loads(code)
			result = code()
		return result
	
	def _wrap(self, client, code):
		''' run and wrap the given code '''
		obj = self._run(client, code)
		if previous := wrapped.get(id(obj)):
			previous.count += 1
		else:
			wrapped[id(obj)] = Wrapped(obj, 0)
		self._own(client, id(obj))
		return id(obj)
		
	def _own(self, client, id, increment=1):
		''' increment the owning counter of the given object '''
		if id in wrapped:
			client.wrapped[id] += increment
			wrapped[id].count += increment
	
	def _drop(self, client, id, increment=1):
		''' decrement the owning counter of the given object '''
		if id in wrapped:
			client.wrapped[id] -= increment
			wrapped[id].count -= increment
			if wrapped[id].count <= 0:
				wrapped.pop(id, None)
		
		
@dataclass
class Client:
	connection: SocketConnection
	wrapped: Counter
	
@dataclass
class Wrapped:
	obj: object
	count: int



def unwrap(address):
	''' get the object referenced by the given address in the global scope '''
	it = iter(address)
	id = next(it)[1]
	try:
		env = wrapped[id].obj
	except KeyError:
		raise ReferenceError("no wrapped object at {:x} in {}, was it dropped by its owners ?".format(id, sid))
	for kind, sub in it:
		if kind == ATTR:	env = getattr(env, sub)
		elif kind == ITEM:	env = env[sub]
		else:
			raise ValueError("the element kind must be either 'attr' or 'item'")
	return env
	
def call(address, args, kwargs):
	''' call the object referenced by the given address in the global scope '''
	return unwrap(address)(*args, **kwargs)


