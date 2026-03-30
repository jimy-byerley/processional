from functools import reduce
from operator import mul
from processional import slave

if __name__ == "__main__":
    process = slave()
    process.invoke(lambda: print('hello from slave'))