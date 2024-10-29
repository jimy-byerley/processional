import struct, socket, pickle
from io import BytesIO as StringIO

from .shared import Pickler


class SerializationError(Exception):  pass


class SocketConnection(object):
	''' convenient structure that wraps a socket to receive/send pickled objects 
		It implements the same interface as a multiprocessing.Connection pipe object, and is meant to be used instead in SlaveProcess if a socket communications is needed.
		
		This class is not thread-safe
	'''
	__slots__ = 'socket', 'tmp', 'current', 'pending'
	
	header = struct.Struct('I')
	max_concat = 512
	max_temp = 4096
	
	def __init__(self, socket, timeout=None):
		self.socket = socket
		self.tmp = memoryview(bytearray(self.max_temp))
		self.current = 0
		self.pending = 0
		self.socket.settimeout(timeout)
		
	def __del__(self):
		self.close()
		
	def close(self):
		self.socket.close()
		
	def recv(self, timeout=None):
		''' receive an object, blocking operation '''
		if self.pending - self.current < self.header.size:
			self._recv_raw(True)
		size, *_ = self.header.unpack(self._read(self.header.size, True))
		data = memoryview(bytearray(size))
		
		start = self._read(min(size, self.pending-self.current), False)
		received = len(start)
		data[:received] = start
		
		while received < size:
			increment = self.socket.recv_into(data[received:], size-received)
			received += increment
			
			if increment <= 0:
				raise EOFError('socket closed during transmission')
		assert received == size
		
		try:
			return pickle.loads(data)
		except Exception as err:
			raise SerializationError('while receiving: {}'.format(err))
		
	def poll(self, timeout=0):
		''' check for data availability.
		
			if timeout is non null or None, the operation is blocking
		'''
		if not self.pending > self.current:
			if timeout is not None:
				self.socket.settimeout(timeout)
			self._recv_raw(True)
			if timeout is not None:
				self.socket.settimeout(None)
		return self.pending > self.current
	
	def send(self, data):
		''' send data '''
		try:	
			file = StringIO()
			Pickler(file).dump(data)
			data = file.getvalue()
		except Exception as err:
			raise SerializationError('while sending,', err)
		
		if len(data) < self.max_concat:
			data = self.header.pack(len(data)) + data
		else:
			self.socket.sendall(self.header.pack(len(data)))
		# blocking send until everything is guaranteed sent
		self.socket.sendall(data)
		
	def _recv_raw(self, blocking=True):
		assert self.pending >= self.current
		if self.current > len(self.tmp)//2:
			self.tmp[:self.pending-self.current] = self.tmp[self.current:self.pending]
			self.pending -= self.current
			self.current = 0
		
		if self.pending >= len(self.tmp):
			return
		
		try:
			increment = self.socket.recv_into(self.tmp[self.pending:], flags=0 if blocking else socket.MSG_DONTWAIT)
			self.pending += increment
		except (BlockingIOError, socket.timeout):
			return
		
		if blocking and increment <= 0:
			raise EOFError('socket closed')
		
	def _read(self, nbytes, blocking=False):
		if self.pending - self.current < nbytes:
			raise ValueError('no enough data received')
		current = self.current
		self.current += nbytes
		return self.tmp[current:self.current]
	
def guess_socket_familly(address):
	if isinstance(address, (bytes, str)):
		return socket.AF_UNIX
	elif isinstance(address, tuple):
		return socket.AF_INET
	else:
		raise TypeError("address must be either a tuple (ip, port) for internet addresses, or string or bytes for unix addresses, not {}".format(type(address)))
	
