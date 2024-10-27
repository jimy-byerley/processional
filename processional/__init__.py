''' 
	Module providing an easy way to distribute tasks to threads and remote processes using functional programming style.
	
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
	- `export` wrap an object in the current thread for use by a remote process
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
	
	Module content
	--------------
'''

__version__ = '0.1'
__docformat__ = 'google'
__all__ = [
	'thread', 'current_thread',
	'Thread', 'SlaveThread', 
	'slave', 'server', 'client', 'serve', 'SlaveProcess', 
	'export', 'RemoteObject',
	'sharedmemory', 'SharedMemory', 
	]

from . import threading, processing, shared, host
from .threading import *
from .processing import *
from .shared import *
