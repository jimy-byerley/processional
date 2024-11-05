# Processional

This module brings the ease and clearity of [functionnal programming](https://en.wikipedia.org/wiki/Functional_programming) into the world of multiprocessing and multithreading in [Python](https://python.org)

> The name stands for *functIONNAL multiPROCESSing*

[![support-version](https://img.shields.io/pypi/pyversions/processional.svg)](https://img.shields.io/pypi/pyversions/processional)
[![PyPI version shields.io](https://img.shields.io/pypi/v/processional.svg)](https://pypi.org/project/processional/)
[![Documentation Status](https://readthedocs.org/projects/processional/badge/?version=latest)](https://processional.readthedocs.io/en/latest/?badge=latest)

## Motivations

The project goals are basically:

- calling a function in a separate thread should be as easy as calling it in the current thread
- calling a function in a separate process should be as easy as calling it in a thread



Starting with notorious alternatives, from a user perspective:

- [`multiprocessing`](https://docs.python.org/3/library/multiprocessing.html), [`mpi`](https://mpi4py.readthedocs.io/en/stable/) and other multiprocessing tools are not functionnal style
  - can only run a file or a module passed, no random tasks
  - several code lines are needed to spawn processes
  - and a lot needs to be done to synchronize and communicate and stop them properly
- [`threading`](https://docs.python.org/3/library/threading.html#module-threading) and other threading tools are not functionnal style
  - can only run one function, no random tasks
  - ignore any returned result or raised exception
  - a lot needs to be done to synchronize threads tasks and to stop them properly
- [`grpc`](https://grpc.io/) or similar remote process call systems bring a lot of friction
  - allows only certain date types to be passed between processes
  - needs a lot of work to wrap functions from server side
- [`multiprocessing`](https://docs.python.org/3/library/multiprocessing.html) performances are unsatisfying
  - slow to spawn threads
  - often wastes RAM
  - needs a server process to manage pipes and shared memories

`processional` aims to bring an answer to these problems using the functionnal and asynchronous programming paradigms and the dynamic languages features

- tasks sent to threads or processes are regular python functions (lambdas, methods, etc)
- tasks can as easily be blocking or background for the master sending the orders
- every tasks report its return value and exceptions
- slaves (threads or processes) are considered as ressources and by default cleaned and stopped when their master drops their reference
- any picklable object can be passed between processes, serialization and shared memory are nicely working together
- proxy objects allows to wrap remote process objects and their methods with no declarations 
- the library is very powerfull with only few user functions



Since [Colesbury brought a solution to the GIL](https://github.com/colesbury/nogil/) , splitting a python program across processes to acheive parallelism will soon no longer be required, so this module will loose a bit of its interest. Anyway this library also features threads, and parallelism is not the only reason of multiprocessing so this project does not seem vain.

## Examples

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

But sometimes you need to keep your heavy resource alive for several tasks. You might simply keep a reference to this resource in your main thread and run a second thread later, but some objects (like Gui objects, or any non thread-safe objects) need to be run or held by one only thread. So you have to communicate your orders to this thread: this is called thread invocation.
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

## Security

While multiprocessing, this library uses [`pickle`](https://docs.python.org/3/library/pickle.html) to send objects between processes and thus TRUST the remote side completely. *Do not use this library to control tasks on a remote machine you do not trust.*

Since SSL tunelling is not yet implemented here, *do not use this library either if the communication between processes can be intercepted (network or OS)*

Basically this library is meant to be used when all processes remote or not are communicating in a secured and closed environment, just like components in one computer.

## Compatibility

| Feature                                         | Unix<br />Python >= 3.9 | Windows<br />Python >= 3.9 |
| ----------------------------------------------- | ----------------------- | -------------------------- |
| threads with results                            | X                       | X                          |
| slave threads                                   | X                       | X                          |
| interruptible threads                           | X                       | X                          |
| slave process                                   | X                       |                            |
| server process through tcp/ip (local or remote  | X                       | X                          |
| server process through unix sockets (faster)    | X                       |                            |
| shared memory                                   | X                       |                            |

## Maturity

This project in its published version has only been tested on small applications. However one of its previous and less complete version had been running programs with ~20 threads and ~10 processes exchanging very frequently all the time (big images, complex data structures, etc) on an industrial machine for over 2 years with no issue.

## Thanks

All this is made possible by 
- the python interpreter's unique level of dynamicity
- [dill](https://github.com/uqfoundation/dill) which extends [pickle](https://docs.python.org/3/library/pickle.html) to serialize functions as long as they are deserialized in the same python interpreter version and environment. After all in interpreted languages, functions are just data
