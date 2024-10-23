# Processional

This module brings the ease and clearity of [functionnal programming](https://en.wikipedia.org/wiki/Functional_programming) into the world of multiprocessing and multithreading in [Python](https://python.org)

## motivations

Starting with other solutions:

- `multiprocessing` , `mpi` and other multiprocessing tools are not functionnal
  - can only run a file or a module passed, no random tasks
  - several code lines are needed to spawn processes
  - and a lot needs to be done to synchronize and communicate and stop them properly
- `threading` and other threading tools are not functionnal style
  - can only run a function
  - drops any returned result or raised exception
  - a lot needs to be done to synchronize threads tasks and to stop them properly
- `grpc` or similar remote process call systems bring a lot of friction
  - allows only certain date types to be passed between processes
  - needs a lot of work to wrap functions from server side
- `multiprocessing` performances are unsatisfying
  - slow to spawn threads
  - often wastes RAM
  - needs a server process to manage pipes and shared memories

`processional` aims to bring an answer to these problems using the functionnal programming paradigm and the dynamic language features

- tasks sent to threads or processes are regular python functions (lambdas, methods, etc)
- tasks can as easily be blocking or background for the master sending the orders
- every tasks report its return value and exceptions
- slaves (threads or processes) are considered as ressources and by default cleaned and stopped when their master drops their reference
- any picklable object can be passed between processes
- proxy objects allows to wrap remote process objects and their methods with no declarations 
- the library is very powerfull with only few user functions

## example

TODO

## security

When multiprocessing, this library uses `pickle` to serialize objects between processes and thus TRUST the remote side completely. Do not use this library to control tasks on a remote machine you do not trust. 

Since SSL sockets are not yet implemented, do not use it either if the communication between processes can be intercepted (network or OS)

Basically this library is meant to be used when all processes remote or not are communicating in a secured and closed shell, just like components in one computer.

## compatiblity

| Feature                                      | Unix<br />Python >= 3.8 | Windows<br />Python >= 3.8 |
| -------------------------------------------- | ----------------------- | -------------------------- |
| threads with results                         | X                       | X                          |
| slave threads                                | X                       | X                          |
| interruptible threads                        | X                       | X                          |
| slave process                                | X                       | X                          |
| server process through tcp/ip                | X                       | X                          |
| server process through unix sockets (faster) | X                       |                            |
| shared memory                                | X                       |                            |

