import os, stat, mmap, copyreg, pickle, ctypes
from weakref import WeakValueDictionary

import dill


__all__ = ['SharedMemory', 'sharedmemory']

shared_memories = WeakValueDictionary()



class SharedMemory(mmap.mmap):
	''' subclass of mmap that can create the mapped file with the desired size or content.
		
		Note:
			- In order to avoid remapping the same file multiple times in a process, use function `sharedmemory()` that has a caching mechanism
			- the `SharedMemory` instance that created the mapped file will delete it on deinitialization. If that file is still mapped in other processes but is a RAM file, the RAM space will continue to exist until dropped by all processes.
		
		Args:
			content:
			
				if provided the file will be created or rewritten with this
				- if `content` is an integer, this is the new size for the file (and memory allocated)
				- if `content` is a buffer object, it is written to the file and the shared memory will have its size
				
			filename:
			
				if provided, this file will be opened or created as shared memory, place it under '/dev/shm' to have a RAM file (best access time)
				if not provided, content must be provided and a unique file name will be chosen and placed in RAM
				
		Attributes:
			file:	the opened mapped file
			owning: set to True if the file has been created on the instance creation, the file will then be removed on instance deinitialization
	'''
	def __new__(self, content=None, filename:str=None):
		if content is not None:
			if filename is None:
				filename = '/dev/shm/processional-'+hex(id(self))
				
			with open(filename, 'wb') as f:
				os.fchmod(f.fileno(), stat.S_IRUSR | stat.S_IWUSR)
				if isinstance(content, int):
					f.write(bytearray(content))
				else:
					f.write(content)
			self.owning = True			
		else:
			self.owning = False
		
		self.file = open(filename, 'r+')
		return super().__new__(self, self.file.fileno(), 0)
	
	def __del__(self):
		self.close()
		self.file.close()
		if self.owning:
			self.owning = False
			try:	os.remove(self.file.name)
			except OSError: pass
			
	def __repr__(self):
		return '{}(filename={})'.format(type(self).__name__, repr(self.file.name))


def sharedmemory(content=None, filename:str=None) -> SharedMemory:
	''' constructor of `SharedMemory` 
		if `content` is None and the given file has already been opened and mapped in this process, the later `SharedMemory` instance is returned instead of creating a new one
		
		See `SharedMemory` for arguments definitions
		
		Example:
			
			>>> shared = sharedmemory(1024)   # allocate 1024 bytes
			>>> view = np.frombuffer(shared, dtype=np.float32)
			
			>>> process.invoke(lambda: print(view))   # no copy
			
			>>> @process.invoke
			... def job():
			...     view[0] = 5
			>>> view
			array([ 5, 0, 0 ... ])
			
		Note:
		
			Nothing prevents concurrent read/write access to a sharedmemory. The user must take care to synchronize accesses using for instance a `RemoteObject` wrapping a `Lock`
	'''
	if content is None:
		if mem := shared_memories.get(filename):
			return mem
	mem = SharedMemory(content, filename)
	shared_memories[mem.file.name] = mem
	return mem



def dump_sharedmemory(obj):
	return sharedmemory, (None, obj.file.name)
	
try:
	import numpy.core as np
except ImportError:	pass
else:
	def try_dump_ndarray(obj):
		# get the object actually owning the buffer
		base = parent = obj
		while True:
			if isinstance(base, (np.ndarray, np.void)):
				base, parent = base.base, base
			elif isinstance(base, memoryview):
				base, parent = base.obj, base
			else:
				break
		# case where it is possible to serialize out-of-band
		if isinstance(base, SharedMemory):
			#print('dump sharedmemory', obj.size, object.__repr__(obj), object.__repr__(base))
			offset = obj.ctypes.data - ctypes.addressof(ctypes.c_byte.from_buffer(base))
			return (obj.dtype, base.file.name, offset, obj.shape, obj.strides)
		elif isinstance(base, mmap.mmap) and isinstance(parent, np.ndarray) and hasattr(parent, 'filename'):
			offset = obj.ctypes.data - ctypes.addressof(ctypes.c_byte.from_buffer(base))
			return (obj.dtype, parent.filename, offset, obj.shape, obj.strides)
		# else it must be serialized in-band

	def dump_shared_ndarray(obj):
		# WARNING: if this dump is used on a subclass of ndarray viewing a shared memory, the dump will share memory but will produce a base ndarray instead of the exact subclass expected
		if args := try_dump_ndarray(obj):
			return load_shared_ndarray, args
		else:
			return obj.__reduce__()

	def dump_shared_void(obj):
		if args := try_dump_ndarray(np.asanyarray(obj).reshape(1)):
			return load_shared_void, args
		else:
			return obj.__reduce__()

	def load_shared_ndarray(dtype, filename, offset, shape, strides):
		shared = sharedmemory(filename=filename)
		return np.ndarray(shape, dtype, shared, offset, strides)

	def load_shared_void(*args):
		return load_shared_ndarray(*args)[0]


class Pickler(pickle.Pickler):
	''' overloaded pickle '''
	dispatch_table = copyreg.dispatch_table.copy()
	dispatch_table[SharedMemory] = dump_sharedmemory
	try:
		import numpy.core as np
	except ImportError:	pass
	else:
		dispatch_table[np.ndarray] = dump_shared_ndarray
		dispatch_table[np.void] = dump_shared_void
	
class Diller(dill.Pickler):
	''' overloaded dill '''
	dispatch_table = copyreg.dispatch_table.copy()
	dispatch_table[SharedMemory] = dump_sharedmemory
	try:
		import numpy.core as np
	except ImportError:	pass
	else:
		dispatch_table[np.ndarray] = dump_shared_ndarray
		dispatch_table[np.void] = dump_shared_void





def benchmark_sharedmemory():
	import numpy as np
	from time import perf_counter
	
	import multiprocessing.shared_memory
	
	import mmap
	
	class KustomShared:
		def __init__(self, size):
			if not os.path.exists('/dev/shm/test'):
				f = open('/dev/shm/test', 'wb')
				f.write(bytearray(size))
			f = open('/dev/shm/test', 'r+')
			self.buf = mmap.mmap(f.fileno(), 0)
			self.size = size
		
		def __reduce__(self):
			return type(self), (self.size,)
	
	# benchmark of memory sharing
	process = server(address=b'/tmp/truc')
	size = 10_000_000
	n = 10
	
	# multiprocessing sharing
	#shared = mp.shared_memory.SharedMemory(create=True, size=size)
	#array = np.asanyarray(shared.buf)
	
	# handmade sharing
	#f = open('/dev/shm/test', 'wb')
	#f.write(big)
	#f = open('/dev/shm/test', 'r+')
	#shared = mmap.mmap(f.fileno(), 0)
	#array = np.asanyarray(shared)
	
	# kustom sharing
	#shared = KustomShared(size)
	#array = np.frombuffer(shared.buf, dtype=np.uint8)
	
	# custom mmap sharing
	shared = sharedmemory(size)
	array = np.frombuffer(shared, dtype=np.uint8)[10::2]
	#array = np.frombuffer(shared, dtype=[('a', 'u1', (4,2)), ('b', 'i2')])['a']
	assert array.base.base.obj is shared
	
	@process.invoke
	def test():
		assert isinstance(array.base, SharedMemory)
	
	array[:] = 1
	local = b'\x01'*size
	
	remote = process.wrap(dict)
	
	start = perf_counter()
	for i in range(n):
		remote['truc'] = local
	print('buffer copy pickle: ', (perf_counter()-start)/n)
	
	start = perf_counter()
	for i in range(n):
		remote['truc'] = shared
	print('buffer shared pickle: ', (perf_counter()-start)/n)
	
	start = perf_counter()
	for i in range(n):
		remote['truc'] = array
	print('view shared pickle: ', (perf_counter()-start)/n)
	
	#start = perf_counter()
	#nothing = remote.wrap(lambda: (lambda x:x))
	#for i in range(n):
		#nothing(array)
	#print('view returned pickle: ', (perf_counter()-start)/n)
	
	start = perf_counter()
	for i in range(n):
		process.invoke(lambda: len(local))
	print('buffer copy dill: ', (perf_counter()-start)/n)
	
	start = perf_counter()
	for i in range(n):
		process.invoke(lambda: len(shared))
	print('buffer shared dill: ', (perf_counter()-start)/n)
	
	start = perf_counter()
	for i in range(n):
		process.invoke(lambda: len(array))
	print('view shared dill: ', (perf_counter()-start)/n)
	
	start = perf_counter()
	@process.invoke
	def job():
		for i in range(n):
			array[:] = 2
	print('buffer shared write: ', (perf_counter()-start)/n)
	
	start = perf_counter()
	local = process.wrap(lambda: np.empty(size, np.uint8))
	@process.invoke
	def job():
		for i in range(n):
			local[:] = 2
	print('buffer local write: ', (perf_counter()-start)/n)
	
	del array
	#shared.close()
	process.close()
