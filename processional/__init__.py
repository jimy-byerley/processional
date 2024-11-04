''' 
	Module providing an easy way to distribute tasks to threads and remote processes using functional programming style.
	
	Terms
	-----
	
	- the main difference between **process** and **thread** is that two processes never share their memory (and need to communicate through pipes, sockets, files, memory mapings or so on). Whereas threads in a process are sharing all the process's memory and thus can only exchange references to objects living in this memory
	
	- a **master-slave** relation between processes is when the slave waits and execute orders from the master, but has only one master
	- a **client-server** relation between processes is when the server waits and execute orders from its clients, but can have many clients at the same time
	
	- a **remote process call (RPC)** protocol is a communication layer allowing to call functions from a process runing on a machine from an other process eventually from a different machine
	
	Main classes
	------------
	
	- `SlaveProcess`  allows to trigger and wait executions on a remote dedicated process
	- `SlaveThread`   allows to trigger and wait executions on an other thread, as `SlaveProcess` would
	- `Thread`        thread wrapper that allows to wait and interrupt the thread jobs
	- `RemoteObject`  convenient proxy over an object living in a slave process
	
	Main functions
	--------------
	
	- `thread` runs a thread
	- `slave` creates a slave process
	- `server` creates a server process
	- `serve` creates a thread serving other processes in the current process
	- `export` wrap an object in the current process for use by a remote process
	- `sharedmemory` creates a buffer object that can be shared accoss processes, that can be viewed using `numpy` or `torch` array
	
	Commandline
	-----------
	
	Server processes are also accessible from commandline
	
		$ python -m processional -a localhost:8000
		
	Commandline options are:
	
	```
	python -m processional [-s][-p][-d] [-a ADDRESS] [-m MODULE]

	-a   provide an ip address in format IP:PORT
		or a path to the unix socket file to create
		it must be specified unless -s is set
	-m   provide the name of a python module to use as __main__ module, 
		or a path to a python file to execute as __main__ when initializing the server
		if ommited, an empty module is created
				
	-s    slave mode, just like a server with a single client
	-p    set the server persistent, meaning it won't exit on last client disconnection
	-d    set the server to detach from its parent thus to not exit on last client disconnection

	-h    show this help
	```
	
	
	Examples
	--------

	Everything in this module is about executing heavy things (resources or computations) in the background. 
	a `Thread` is the default choice for this:

	```python
	def heavy_work(x):
		return sum(x)
	
	def foo():
		heavy_resource = list(range(1_000_000))
		return heavy_work(heavy_resource)
	
	# use a thread to perform this task
	task = thread(foo)
	print('and the result is ...')
	print(task.wait())
	```
	```
	and the result is ...
	499999500000
	```

	But sometimes you need to keep your heavy resource alive for several tasks. You might simply keep a reference to this resource in your main thread and run a second thread later, but some objects (like Gui objects, or non thread-safe object) need to be run or held only in one only thread. So you have to communicate your orders to this thread: this is called invocation or scheduling.
	a `SlaveThread` is the way to work around this:

	```python
	from functools import reduce
	from operator import mul

	thread = SlaveThread()
	heavy_resource = thread.invoke(lambda: list(range(1_000_000)))
	heavy_work_1 = thread.schedule(lambda: sum(heavy_resource))
	heavy_work_2 = thread.schedule(lambda: reduce(mul, heavy_resource))
	print('summing')
	print('multiplying')
	print('sum is', heavy_work_1.wait())
	print('product is', heavy_work_2.wait())
	```
	```
	summing
	multiplying
	sum is 499999500000
	product is 0
	```

	A further need is to achieve the same as the two previous, but on a remote computer or in a separate process (in order to distribute the load on a network, or to gain parallelism, or to benefit from other machine's hardwares). This is called *Remote Process Call (RPC)*
	a `SlaveProcess` is answering this need:

	```python
	from functools import reduce
	from operator import mul

	process = slave() # spawn and connect, the result is a SlaveProcess
	heavy_resource = process.wrap(lambda: list(range(1_000_000)))
	heavy_work_1 = process.schedule(lambda: sum(heavy_resource))
	heavy_work_2 = process.schedule(lambda: reduce(mul, heavy_resource))
	print('summing')
	print('multiplying')
	print('sum is', heavy_work_1.wait())
	print('product is', heavy_work_2.wait())
	```
	```
	summing
	multiplying
	sum is 499999500000
	product is 0
	```

	A last need is to send commands from multiple machines or processes to the same remote process (allowing multiple machines to use a shared ressources in the remote process). In this situation, the communication style changes from master-slave to client-server
	a `SlaveProcess` also handles that:

	```python
	# in process 1
	process = server('/tmp/server') # spawn and connect, the result is a SlaveProcess
	@process.schedule
	def init_global():
		global heavy_resource
		heavy_resource = list(range(1_000_000))
	
	heavy_work_1 = process.schedule(lambda: sum(heavy_resource))
	print('summing')
	print('sum is', heavy_work1.wait())
	```
	```python
	# in process 2
	from functools import reduce
	from operator import mul

	process = client('/tmp/server') # connect, the result is a SlaveProcess
	heavy_work_2 = process.schedule(lambda: reduce(mul, heavy_resource))
	print('multiplying')
	print('product is', heavy_work_2.wait())
	```
	```
	summing
	multiplying
	sum is 499999500000
	product is 0
	```

	
	Module content
	--------------
'''

__version__ = '0.1'
__docformat__ = 'google'
__all__ = [
	'thread', 'current_thread', 'main_thread',
	'Thread', 'SlaveThread', 
	'slave', 'server', 'client', 'serve', 'SlaveProcess', 
	'export', 'RemoteObject',
	'sharedmemory', 'SharedMemory', 
	]

from . import threading, processing, shared, host
from .threading import *
from .processing import *
from .shared import *
