'''
	entry point to run a slave from `eve.process` from commandline
'''

from .host import Server
from .process import _default_address
import sys, os

raw_address = None
raw_module = None
persistent = False
detach = False
single = False
help = False

it = iter(sys.argv)
next(it)
for arg in it:
    if arg == '-p':
        persistent = True
    elif arg == '-d':
        detach = True
    elif arg == '-s':
        single = True
    elif arg == '-h':
        help = True
    elif arg == '-a':
        raw_address = next(it, None)
    elif arg == '-m':
        raw_module = next(it, None)

if help:
    print('''usage: python -m processional [-s][-p][-d] [-a ADDRESS] [-m MODULE]

-a   provide an ip address in format IP:PORT
     or a path to the unix socket file to create
     it must be specified unless -s is set
-m   provide the name of a python module to use as __main__ module, 
     or a path to a python file to execute as __main__
     if ommited, an empty module is created
            
-s    slave mode, just like a server with a single client
-p    set the server persistent, meaning it won't exit on last client disconnection
-d    set the server to detach from its parent thus to not exit on last client disconnection
''')
    sys.exit(1)

# become independent of the signals sent to the parent process
try:	os.setsid()
except PermissionError:	
    warnings.warn("unable to set process signal group, the slave will receive same signals as its parent")

if not raw_module:
    import types
    module = types.ModuleType('__mp_main__')
elif raw_module.isidentifier():
    import importlib
    module = importlib.import_module(raw_module)
elif os.path.exists(raw_module):
    import types
    sys.path.append(os.path.dirname(raw_module))
    module = types.ModuleType('__mp_main__')
    module.__file__ = raw_module
    sys.modules[module.__name__] = module
    sys.modules['__main__'] = module
    exec(open(raw_module, 'r').read(), module.__dict__)
else:
    raise IOError('unable to find {}'.format(repr(raw_module)))

if not raw_address:
    address = _default_address(os.getpid())

if ':' in raw_address:
    ip, port = raw_address.split(':')
    port = int(port)
    address = (ip, port)
else:
    address = raw_address

sys.modules[module.__name__] = module
sys.modules['__main__'] = module
Server(address, module, persistent=persistent, attached=not detach).loop()