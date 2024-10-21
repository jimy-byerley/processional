''' 
	Module providing an easy way to distribute tasks to threads and remote processes using functional programming style.
	
	Main classes
	------------
	
	- `SlaveProcess`  allows to trigger and wait executions on a remote dedicated process
	- `SlaveThread`   allows to trigger and wait executions on an other thread, as `SlaveProcess` would
	- `Thread`        thread wrapper that allows to wait and interrupt the thread jobs
	
	Some features of this module are also accessible from commandline
	
		$ python -m processional.slave localhost:8000 main/pick_applenator_ultraparallel.py

'''

from time import time, sleep
from types import CodeType, FunctionType
from multiprocessing import Process, Pipe
from threading import Lock, Condition, current_thread
import threading
import socket


from weakref import WeakValueDictionary
from collections import deque
from pnprint import nprint
import dill

import warnings
import traceback
import os
import sys
import ctypes
import select
import struct
import pickle



__all__ = [
	'Thread', 'spawn', 'thread',
	'SlaveThread', 
	'SlaveProcess', 'RemoteObject', 'LocalObject', 'slave', 'server', 'client', 'localserver', 'localwrap', 
	'SharedMemory', 'sharedmemory',
	]

from .thread import *
from .process import *
from .shared import *


