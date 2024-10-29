import sys, types, traceback
import os, subprocess
import re


def module_eval(module, expand_passed=False, expand_failed=True, dynamic=False, isolate=False):
	if dynamic: tests = module_explore_dynamic(module)
	else:       tests = module_explore_static(module)
	
	for module, func in tests:
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

def module_explore_dynamic(module):
	__import__(module)
	module = sys.modules[module]
	for name, value in list(module.__dict__.items()):
		if callable(value) and name.startswith('test_'):
			yield (module.__name__, value.__name__)
		elif isinstance(value, types.ModuleType) and value.__name__.startswith(module.__name__):
			yield from module_explore_dynamic(value.__name__)

def module_explore_static(module):
	for file in os.listdir(module):
		file = os.path.join(module, file)
		if os.path.isdir(file):
			yield from module_explore_static(file)
		elif file.endswith('.py'):
			for func in re.findall(r'^def\s+(test_\w+)', open(file, 'r').read(), flags=re.MULTILINE):
				yield (file.replace('.py', '').replace(os.path.sep, '.'), func)

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

if __name__ == '__main__':
	module = sys.argv[1]
	path = sys.argv[1].split('.')
	if path[-1].startswith('test_'):
		func = path.pop()
		module = '.'.join(path)
		success, outs = func_eval_local(module, func)
		print_outs(outs)
	else:
		module_eval(
			module, 
			dynamic='-d' in sys.argv,
			isolate='-i' in sys.argv,
			expand_failed='-f' in sys.argv,
			expand_passed='-p' in sys.argv,
			)
