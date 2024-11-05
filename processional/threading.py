import sys, threading, ctypes, traceback
from threading import Lock, Condition, current_thread, main_thread
from collections import deque


__all__ = ['thread', 'current_thread', 'main_thread', 'Thread', 'SlaveThread']


def thread(func, detach=False) -> 'Thread':
	''' spawn a thread running the given function 
	
		Args:
			func:  the function run by the thread, its result or errors are propagated to `Thread.wait()`
			detach:  if `False`, the thread will be set as daemon and stopped automatically when the process's main thread ends
	'''
	thread = Thread(target=func, daemon=not detach)
	thread.start()
	return thread
	



class Thread(threading.Thread):
	''' a thread object that returns a result and handles exceptions better than `threading.Thread`
	
		It is often more convenient to instantiate it using `thread`
	
		This class is thread-safe
		
		Example:
		
			Spawn a thread in a functional or in a decorator style
		
			>>> def myfunc():
			...     sleep(1)
			...     return 'ok'
			>>> mythread = thread(myfunc)
		
			>>> @thread
			... def mythread():
			...     sleep(1)
			...     return 'ok'
			
			Wait its result
			
			>>> mythread.complete()
			False
			>>> result = mythread.wait()
			>>> result
			'ok'
			
			Errors are propagated
			
			>>> root = thread(lambda: sqrt(-1))
			>>> root.wait()
			Traceback (most recent call last):
			File ".../test.py", line 5, in <module>
				root.wait()
			File ".../processional/processional/threading.py", line 138, in wait
				raise self.error
			File ".../processional/processional/threading.py", line 105, in run
				self.result = self.target()
							^^^^^^^^^^^^^
			File ".../test.py", line 4, in <lambda>
				root = thread(lambda: sqrt(-1))
									^^^^^^^^
			ValueError: math domain error
	'''
	def __init__(self, target:callable, daemon=None, name:str=None, warnignore=False, warnerror=False):
		''' the constructor creates the thread object, but doesn't start it 
		
			Args:
				target:      the callable the thread will execute
				daemon:      if True, the thread will automatically exit with the main thread
				name:        a name for the thread (optional, just for debug purpose)
				warnignore:  if True, each ignored interruption will issue a warning
				warnerror:   if True, a fatal exception in the thread will be printedout (event if handled afterward)
		'''
		super().__init__(daemon=daemon, name=name)
		self.error = RuntimeError('thread terminated')
		self.target = target
		self.result = None
		self.checked = False
		
		self.warnerror = warnerror
		self.warnignore = warnignore
		
		self.interruptlock = Lock()
		self.interruptable = False
		self.interruption = None
		
	def __repr__(self):
		if not self.is_alive():
			if self.error:		state = 'aborted'
			else:				state = 'complete'
		else:	state = 'running'
		if self.daemon:	state = 'daemon '+state
		return '<{} {}, {}>'.format(type(self).__name__, self.ident, state)

	def __del__(self):
		if self.error and not self.checked and not isinstance(self.error, SystemExit):
			print('Exception in', self, file=sys.stderr)
			traceback.print_exception(self.error)
	
	def run(self):
		''' not meant for override, the function to run in the thread must be passed to `self.__init__` '''
		try:
			self.eit()
			self.result = self.target()
		except Exception as err:
			if self.warnerror:
				warnings.warn('exception occured in {}: {}'.format(self, traceback.format_exception(err)))
			self.error = err
		else:
			self.error = None
		if self.interruption:
			self._ignore(self.interruption)
			self.interruption = None
	
	def available(self) -> bool:
		''' return True if the task termination result is available (if the thread ended successfully or with an error) 
			This is a shorthand for `not is_alive()`
		'''
		return not self.is_alive()
	
	def complete(self) -> bool:
		''' return True if the thread ended successfully, False if still running, and raise the exception that stopped the thread if any. '''
		if self.is_alive():
			return False
		self.checked = True
		if self.error:
			raise self.error
		return True
			
	def wait(self, timeout:float=None):
		''' same as `join()` but return the thread function's return value on successfull termination, and raise the exception that stopped the thread if any. '''
		self.join(timeout)
		if self.is_alive():
			raise TimeoutError
		self.checked = True
		if self.error:
			raise self.error
		return self.result
		
	def terminate(self):
		''' interrupt the thread with a `SystemExit` exception '''
		self.checked = True
		if self.is_alive():
			self.interrupt(SystemExit)
			
	def interrupt(self, exception:type):
		''' send the given exception to the thread. That exception will be raised in the thread call stack as soon as the thread become active and interruptable '''
		with self.interruptlock:
			if self.interruptable and self.ident is not None:
				self._throw(exception)
			elif not self.interruption:
				self.interruption = exception
			else:
				self._ignore(exception)
			
	def eit(self, ignore=False):
		''' enable interruptions until `nit()` is called. 
			If `ignore` is True, all pending interruptions `nit` will be ignored.
		'''
		with self.interruptlock:
			self.interruptable = True
			err, self.interruption = self.interruption, None
			if self.interruption:
				if ignore:
					self._ignore(err)
				else:
					raise err
				
	def nit(self):
		''' disable interruptions until `eit()` is called '''
		with self.interruptlock:
			self.interruptable = False
			
	def nointerrupt(self, ignore=False) -> 'NoInterrupt':
		''' provide a context manager to disable interruptions '''
		return self.NoInterrupt(self, ignore)
		
	class NoInterrupt(object):
		''' context manager for locking interruption of a thread, it simply calls `nit` when entering and `eit` when exiting '''
		__slots__ = 'thread', 'ignore'
		def __init__(self, thread, ignore):
			self.thread = thread
			self.ignore = ignore
		def __enter__(self):
			self.thread.nit()
		def __exit__(self):
			self.thread.eit(self.ignore)

	def _throw(self, exctype):
		# can only raise a type (not instantiated)
		if not isinstance(exctype, type):
			exctype = type(exctype)
		if self.ident is None:
			raise AssertionError('thread not started')
			
		res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(self.ident), ctypes.py_object(exctype))
		if res == 0:
			raise ValueError("invalid thread id")
		elif res != 1:
			# according to cpython's documentation:
			# if it returns a number greater than one, you're in trouble, and you should call it again with exc=NULL to revert the effect
			ctypes.pythonapi.PyThreadState_SetAsyncExc(self.ident, 0)
			raise RuntimeError("PyThreadState_SetAsyncExc failed")
			
	def _ignore(self, exception):
		''' procedure for ignoring interruptions '''
		if not self.warnignore:
			if isinstance(exception, type):		message = exception.__name__
			else:								message = '{}: {}'.format(type(exception).__name__, exception)
			warnings.warn('interruption ignored in {}: {}'.format(self, message))


		
class SlaveThread:
	''' thread control object
	
		This class is thread-safe
	
		Attributes:
		
			thread (Thread):  thread executing the commands
			busy (bool):  True if the thread is currently processing a task
	
		Example:
		
			>>> thread = SlaveThread()
			
			Functions, closures are passed to the thread to be executed, the result or error is retreived
			
			>>> thread.invoke(lambda: print('hello'))  # the closure executed in the thread
			>>> task = thread.schedule(lambda: print('hello'))  # does not block
			>>> task.wait()
			
			Errors are propagated
			
			>>> root = thread.schedule(lambda: sqrt(-1))
			>>> root.wait()
			Traceback (most recent call last):
			File ".../test.py", line 6, in <module>
				root.wait()
			File ".../processional/threading.py", line 415, in wait
				raise err
			File ".../processional/threading.py", line 337, in step
				result = task()
						^^^^^^
			File ".../test.py", line 5, in <lambda>
				root = thread.schedule(lambda: sqrt(-1))
											^^^^^^^^
			ValueError: math domain error
			
	'''
	def __init__(self, existing=None):
		self.id = 0		# max task id used
		self.tasks = deque()	# tasks in the queue, as tuples (id, func)
		self.register = {}	# task results indexed by task id
		self.sendsig = Condition()	# signal sent to the thread when a new task is scheduled
		self.recvsig = Condition()	# signal sent by the thread when a task has been completed
		self.lock = Lock()
		self.thread = existing or thread(self.loop)
		self.running = True
		
	def __repr__(self):
		return '<{} ident={}>'.format(type(self).__name__, self.thread.ident)
		
	def __del__(self):
		self.close()
		
	def close(self) -> 'Task':
		''' stop the thread reception loop. 
			It will stop once finished the already scheduled tasks 
		'''
		return self.Task(self, None)
		
	def schedule(self, func:callable) -> 'Task':
		''' schedule a task in the thread, to be executed after previously scheduled tasks
			return a `Task` object that allows to receive the result value
		'''
		if not self.running:
			raise KeyboardInterrupt('slave exited')
		return self.Task(self, func)
	
	def invoke(self, func:callable):
		''' schedule a task then immediately wait for its result '''
		if current_thread() == self.thread:
			return func()
		elif not self.running:
			raise KeyboardInterrupt('slave exited')
		else:
			return self.schedule(func).wait()
		
	def loop(self):
		''' the default thread event loop 
			It will block until the master has closed (or dropped) the slave
		'''
		while self.step():
			if not self.tasks:
				with self.sendsig:
					self.sendsig.wait()
		self.abort()
	
	def step(self) -> bool:
		''' execute all already scheduled tasks
			This is meant to be called periodically by the thread event loop
			
			return False if the thread should not wait for tasks anymore (the slave must be closed). Note that if you are calling this method from a custom event loop, you are not obliged to quit the thread, that's just telling no other task will be sent.
		'''
		self.busy = True
		while self.tasks:
			id, task = self.tasks.popleft()
			if task is None:
				return False
			
			error = result = None
			try:	
				result = task()
			except Exception as err:
				error = err
			if id in self.register:
				self.register[id] = (error, result)
			elif error:
				print('Exception in', self, file=sys.stderr)
				traceback.print_exception(error)
			with self.recvsig:
				self.recvsig.notify_all()
		self.busy = False
		return True
					
	def abort(self, exception:type=None):
		''' abort all scheduled tasks, sending them a `KeyboardInterrupt` 
			This should be called by the event loop while exiting
		'''
		self.running = False
		if not exception: 
			exception = KeyboardInterrupt('slave exited')
		while self.tasks:
			id, task = self.tasks.popleft()
			self.register[id] = (exception, None)
		with self.recvsig:
			self.recvsig.notify_all()
		self.busy = False
			
	
	class Task(object):
		''' simple scheduled task awaiter '''
		__slots__ = 'id', 'slave'
		
		def __init__(self, slave, func):		
			self.slave = slave
			with self.slave.lock:
				self.slave.id += 1
				self.id = self.slave.id
			self.slave.tasks.append((self.id, func))
			self.slave.register[self.id] = None
			with self.slave.sendsig:
				self.slave.sendsig.notify_all()
			
		def __del__(self):
			termination = self.slave.register.pop(self.id, None)
			if termination:
				err, result = termination
				if err:
					print('Exception in', self, file=sys.stderr)
					traceback.print_exception(err)
			
		def __repr__(self):
			return '<{} {}>'.format(type(self).__name__, self.id)
			
		def available(self) -> bool:
			''' return True if the result is available '''
			return bool(self.slave.register[self.id])
			
		def complete(self) -> bool:
			''' return True if the task is over, False if the task is running and raise the exception if the task encountered and exception '''
			termination = self.slave.register[self.id]
			if not termination:
				return False
			err, result, report = termination
			if err:
				self.slave.register[self.id] = None
				raise err
			return True
			
		def wait(self, timeout:float=None):
			''' wait for the task to complete on the thread, then return its returned result 
				if `timeout` is not None, it will wait at most `timeout` and then raise a `TimeoutError` if the task is not completed.
			'''
			while not self.slave.register[self.id]:
				with self.slave.recvsig:
					if not self.slave.recvsig.wait(timeout) and timeout is not None:
						raise TimeoutError('no value received in time')
			err, result = self.slave.register.pop(self.id)
			if err:
				raise err
			return result
