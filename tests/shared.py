from processional.shared import Diller, Pickler, sharedmemory
import pickle
from io import BytesIO as StringIO

def test_sharedmemory():
	data = b'12345'*1000
	mem = sharedmemory(data)
	
	try:	pickle.dumps(mem)
	except TypeError:  pass
	else:   assert False
	
	file = StringIO()
	pickler = Pickler(file)
	pickler.dump(mem)
	load = pickle.loads(file.getvalue())
	assert memoryview(mem) == memoryview(load)
	assert memoryview(mem).obj is memoryview(load).obj
	
	file = StringIO()
	pickler = Diller(file)
	pickler.dump(mem)
	load = pickle.loads(file.getvalue())
	assert memoryview(mem) == memoryview(load)
	assert memoryview(mem).obj is memoryview(load).obj
	
