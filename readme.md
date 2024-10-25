# Processional

This module brings the ease and clearity of [functionnal programming](https://en.wikipedia.org/wiki/Functional_programming) into the world of multiprocessing and multithreading in [Python](https://python.org)

> The name stands for *functIONNAL multiPROCESSing*

## motivations

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

## example

TODO

## security

While multiprocessing, this library uses [`pickle`](https://docs.python.org/3/library/pickle.html) to send objects between processes and thus TRUST the remote side completely. *Do not use this library to control tasks on a remote machine you do not trust.*

Since SSL tunelling is not yet implemented here, *do not use this library either if the communication between processes can be intercepted (network or OS)*

Basically this library is meant to be used when all processes remote or not are communicating in a secured and closed environment, just like components in one computer.

## compatiblity

| Feature                                         | Unix<br />Python >= 3.8 | Windows<br />Python >= 3.8 |
| ----------------------------------------------- | ----------------------- | -------------------------- |
| threads with results                            | X                       | X                          |
| slave threads                                   | X                       | X                          |
| interruptible threads                           | X                       | X                          |
| slave process                                   | X                       |                            |
| server process through tcp/ip (local or remote  | X                       | X                          |
| server process through unix sockets (faster)    | X                       |                            |
| shared memory                                   | X                       |                            |

## thanks

All this is made possible by 
- the python interpreter's unique level of dynamicity
- [dill](https://github.com/uqfoundation/dill) which extends [pickle](https://docs.python.org/3/library/pickle.html) to serialize functions as long as they are deserialized in the same python interpreter version and environment. After all functions in interpreted languages is just data
