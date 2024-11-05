#!/usr/bin/python3
import sys, types, traceback
import os, subprocess
import re, time
import tyro


def module_eval(module, dynamic=False, **kwargs):
	if dynamic: tests = module_explore_dynamic(module)
	else:       tests = module_explore_static(module)
	
	start = time.time()
	success = 0
	total = 0
	for module, func in tests:
		total += 1
		success += function_eval(module, func, **kwargs)
	end = time.time()
	
	print('\n──────── {}{} passed{} and {}{} failed{} ({}% passed), executed in {:.2g}s ────────'.format(
		SUCCESS, success, NORMAL,
		ERROR, total-success, NORMAL,
		int(success / (total or 1) * 100),
		end - start,
		))
		
def function_eval(module, func, expand_passed=False, expand_failed=True, isolate=False):
	print('{}testing{} {}.{} ...  '.format(DETAIL, NORMAL, module, func), end='')
	sys.stdout.flush()
	
	if isolate:     success, outs = func_eval_subprocess(module, func)
	else:           success, outs = func_eval_local(module, func)
	
	if success:
		print(f'{SUCCESS}passed{NORMAL}')
		if expand_passed:  print_outs(outs)
	else:
		print(f'{ERROR}failed{NORMAL}')
		if expand_failed:  print_outs(outs)
		
	return success

def module_explore_dynamic(module):
	__import__(module)
	module = sys.modules[module]
	for name, value in list(module.__dict__.items()):
		if callable(value) and name.startswith('test_'):
			yield (module.__name__, value.__name__)
		elif isinstance(value, types.ModuleType) and value.__name__.startswith(module.__name__):
			yield from module_explore_dynamic(value.__name__)

def module_explore_static(module, root=None):
	if root is None:
		root, path = find_module_static(module)
	else:
		path = module
	if os.path.isdir(path):
		for file in os.listdir(path):
			file = os.path.join(path, file)
			yield from module_explore_static(file, root)
	elif path.endswith('.py'):
		for func in re.findall(r'^def\s+(test_\w+)', open(path, 'r').read(), flags=re.MULTILINE):
			yield (path[len(root):].replace('.py', '').replace(os.path.sep, '.'), func)

def find_module_static(module):
	rootfd = module.replace('.', os.path.sep)
	rootpy = rootfd+'.py'
	for folder in sys.path:
		attempt = os.path.join(folder, rootfd)
		if os.path.exists(attempt):	return folder+os.path.sep, attempt
		attempt = os.path.join(folder, rootpy)
		if os.path.exists(attempt):	return folder+os.path.sep, attempt
	raise ModuleNotFoundError('No module named {}'.format(repr(module)))
				
def func_eval_subprocess(module, func):
	result = subprocess.run(
		[sys.executable, '-c', 'import {}; {}.{}()'.format(module, module, func)],
		stdout = subprocess.PIPE,
		stderr = subprocess.PIPE,
		)
	return result.returncode == 0, (result.stdout, result.stderr)

def func_eval_local(module, func):
	__import__(module)
	func = getattr(sys.modules[module], func)
	sys.stdout.flush()
	sys.stderr.flush()
	stdout = sys.stdout
	stderr = sys.stderr
	sys.stdout = open('/tmp/stdout', 'w')
	sys.stderr = open('/tmp/stderr', 'w')
	try:	
		func()
	except KeyboardInterrupt:
		traceback.print_exc()
		success = False
	except Exception:
		traceback.print_exc()
		success = False
	else:
		success = True
	finally:
		sys.stdout.flush()
		sys.stderr.flush()
		sys.stdout = stdout
		sys.stderr = stderr
	return success, (open('/tmp/stdout', 'rb').read(), open('/tmp/stderr', 'rb').read())

def print_outs(result):
	stdout, stderr = result
	if stdout:
		print('  stdout:\n{}\n'.format(stdout.decode('utf-8')).replace('\n', '\n\t'))
	if stderr:
		print('  stderr:\n{}\n'.format(stderr.decode('utf-8')).replace('\n', '\n\t'))
			
NORMAL = '\033[0m'
ERROR = '\033[91m'
SUCCESS = '\033[92m'
DETAIL = '\033[90m'

@tyro.cli
def main(module:str='tests', /, dynamic:bool=False, isolate:bool=False, failed:bool=False, passed:bool=False):
	''' run unit tests present in the given python module
	
	Args:
		module: the python module to test, must be on PYTHONPATH
		dynamic: import the module in order to search of test functions, default is by looking for function names in the .py files
		isolate: run each test in a separate process
		failed: print stdin and stdout for failed tests
		passed: print stdin and stdout for passed tests
	'''
	path = module.split('.')
	if path[-1].startswith('test_'):
		func = path.pop()
		module = '.'.join(path)
		function_eval(module, func)
	else:
		module_eval(
			module, 
			dynamic=dynamic,
			isolate=isolate,
			expand_failed=failed,
			expand_passed=passed,
			)
