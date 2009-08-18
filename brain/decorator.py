##########################	 LICENCE	 ###############################
##
##   Copyright (c) 2005, Michele Simionato
##   All rights reserved.
##
##   Redistributions of source code must retain the above copyright
##   notice, this list of conditions and the following disclaimer.
##   Redistributions in bytecode form must reproduce the above copyright
##   notice, this list of conditions and the following disclaimer in
##   the documentation and/or other materials provided with the
##   distribution.

##   Modified: ported to py3k and removed obsolete parts

"""
Decorator module, see http://pypi.python.org/pypi/decorator
for the documentation.
"""

__all__ = ["decorator", "FunctionMaker"]

import os, sys, re, inspect, warnings
from functools import partial

DEF = re.compile('\s*def\s*([_\w][_\w\d]*)\s*\(')

# basic functionality
class FunctionMaker(object):
	"""
	An object with the ability to create functions with a given signature.
	It has attributes name, doc, module, signature, defaults, dict and
	methods update and make.
	"""
	def __init__(self, func=None, name=None, signature=None,
				 defaults=None, doc=None, module=None, funcdict=None):
		if func:
			# func can also be a class or a callable, but not an instance method
			self.name = func.__name__
			if self.name == '<lambda>': # small hack for lambda functions
				self.name = '_lambda_'
			self.doc = func.__doc__
			self.module = func.__module__
			if inspect.isfunction(func):
				self.signature = inspect.formatargspec(
					formatvalue=lambda val: "", *inspect.getargspec(func))[1:-1]
				self.defaults = func.__defaults__
				self.dict = func.__dict__.copy()
		if name:
			self.name = name
		if signature is not None:
			self.signature = signature
		if defaults:
			self.defaults = defaults
		if doc:
			self.doc = doc
		if module:
			self.module = module
		if funcdict:
			self.dict = funcdict
		# check existence required attributes
		assert hasattr(self, 'name')
		if not hasattr(self, 'signature'):
			raise TypeError('You are decorating a non function: %s' % func)

	def update(self, func, **kw):
		"Update the signature of func with the data in self"
		func.__name__ = self.name
		func.__doc__ = getattr(self, 'doc', None)
		func.__dict__ = getattr(self, 'dict', {})
		func.__defaults__ = getattr(self, 'defaults', ())
		callermodule = sys._getframe(3).f_globals.get('__name__', '?')
		func.__module__ = getattr(self, 'module', callermodule)
		func.__dict__.update(kw)

	def make(self, src_templ, evaldict=None, addsource=False, **attrs):
		"Make a new function from a given template and update the signature"
		src = src_templ % vars(self) # expand name and signature
		evaldict = evaldict or {}
		mo = DEF.match(src)
		if mo is None:
			raise SyntaxError('not a valid function template\n%s' % src)
		name = mo.group(1) # extract the function name
		reserved_names = set([name] + [
			arg.strip(' *') for arg in self.signature.split(',')])
		for n, v in evaldict.items():
			if n in reserved_names:
				raise NameError('%s is overridden in\n%s' % (n, src))
		if not src.endswith('\n'): # add a newline just for safety
			src += '\n'
		try:
			code = compile(src, '<string>', 'single')
			exec(code, evaldict)
		except:
			print('Error in generated code:', file=sys.stderr)
			print(src, file=sys.stderr)
			raise
		func = evaldict[name]
		if addsource:
			attrs['__source__'] = src
		self.update(func, **attrs)
		return func

	@classmethod
	def create(cls, obj, body, evaldict, defaults=None, addsource=True,**attrs):
		"""
		Create a function from the strings name, signature and body.
		evaldict is the evaluation dictionary. If addsource is true an attribute
		__source__ is added to the result. The attributes attrs are added,
		if any.
		"""
		if isinstance(obj, str): # "name(signature)"
			name, rest = obj.strip().split('(', 1)
			signature = rest[:-1]
			func = None
		else: # a function
			name = None
			signature = None
			func = obj
		fun = cls(func, name, signature, defaults)
		ibody = '\n'.join('	' + line for line in body.splitlines())
		return fun.make('def %(name)s(%(signature)s):\n' + ibody,
						evaldict, addsource, **attrs)

def decorator(caller, func=None):
	"""
	decorator(caller) converts a caller function into a decorator;
	decorator(caller, func) decorates a function using a caller.
	"""
	if func is not None: # returns a decorated function
		return FunctionMaker.create(
			func, "return _call_(_func_, %(signature)s)",
			dict(_call_=caller, _func_=func), undecorated=func)
	else: # returns a decorator
		return partial(decorator, caller)
