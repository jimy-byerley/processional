from functools import reduce
from operator import mul
from processional import slave

process = slave() # spawn and connect, the result is a SlaveProcess
# heavy_resource = process.wrap(lambda: list(range(100)))
# heavy_work_1 = process.schedule(lambda: sum(heavy_resource))
# heavy_work_2 = process.schedule(lambda: reduce(mul, heavy_resource))
# print('summing')
# print('multiplying')
# print('sum is', heavy_work_1.wait())
# print('product is', heavy_work_2.wait())