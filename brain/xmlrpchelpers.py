"""
Helper classes for XML RPC used in this DB

TODO: As compared to original XML RPC, keyword arguments support was added
Probably it should be eliminated.
"""

from xmlrpc.server import DocXMLRPCServer
from xmlrpc.client import ServerProxy, Binary, Fault, MultiCall, MultiCallIterator
import xmlrpc.client
import re

def _transformBinary(data, back=False):
	"""
	Transform nested data, changing each bytes occurrence to Binary
	(or vice versa if back == True)
	"""
	actions = {
		dict: lambda x: {key: _transformBinary(x[key], back) for key in x},
		list: lambda x: [_transformBinary(elem, back) for elem in x],
		tuple: lambda x: tuple(_transformBinary(elem, back) for elem in x)
	}

	if back:
		actions[Binary] = lambda x: bytes(x.data)
	else:
		actions[bytes] = Binary

	if type(data) in actions:
		return actions[type(data)](data)
	else:
		return data

def _parseFault(fault_code, fault_string, exceptions):
	"""Parse XML RPC Fault object and get known exception from it"""

	_error_pat = re.compile('(?P<exception>[^:]*):(?P<rest>.*$)')
	m = _error_pat.match(fault_string)
	if m:
		exception_name = m.group('exception')
		rest = m.group('rest')
		for exc in exceptions:
			if repr(exc) == exception_name:
				raise exc(rest)

	# Fall through and just raise the fault
	raise xmlrpc.client.Fault(fault_code, fault_string)


class _MyServerProxyMethod:
	"""
	Wrapper for method calls for MyServerProxy
	- adds keyword arguments as a dictionary parameter
	- transforms bytes to Binary and back
	- raises known exceptions instead of Fault
	"""

	def __init__(self, send, name, exceptions):
		self.__send = send
		self.__name = name
		self.__exceptions = exceptions

	def __call__(self, *args, **kwds):
		l = list(args)
		l.append(kwds)
		l = _transformBinary(l)
		args = tuple(l)

		try:
			res = self.__send.__getattr__(self.__name)(*args)
		except Fault as f:
			_parseFault(f.faultCode, f.faultString, self.__exceptions)

		res = _transformBinary(res, back=True)
		return res

	def __getattr__(self, name):
		return _MyServerProxyMethod(self.__send, self.__name + '.' + name, self.__exceptions)


class MyServerProxy:
	"""Wrapper for XML RPC client, used for DB"""

	def __init__(self, *args, **kwds):
		if 'exceptions' in kwds:
			exceptions = kwds['exceptions']
			del kwds['exceptions']
		else:
			exceptions = []
		self._exceptions = exceptions

		kwds['allow_none'] = True

		self.s = ServerProxy(*args, **kwds)

	def __getattr__(self, name):
		return _MyServerProxyMethod(self.s, name, self._exceptions)


class _KeywordInstance:
	"""Wrapper for object method calls with keyword arguments, server side"""

	def __init__(self, inst):
		self._inst = inst

	def _dispatch(self, method, params):
		args = list(params)
		args = _transformBinary(args, back=True)
		kwds = args.pop()
		args = tuple(args)
		res = self._inst._dispatch(method, *args, **kwds)
		res = _transformBinary(res)
		return res

	def __getattr__(self, name):
		return getattr(self._inst, name)


class _KeywordFunction:
	"""Wrapper for function calls with keyword arguments, server side"""

	def __init__(self, func):
		self._func = func

	def __call__(self, *args):
		args = list(args)
		kwds = args.pop()
		args = tuple(args)
		return self._func(*args, **kwds)


class MyXMLRPCServer(DocXMLRPCServer):
	"""Wrapper for XML RPC server, used for DB"""

	def __init__(self, *args, **kwds):
		kwds['allow_none'] = True
		kwds['logRequests'] = False
		DocXMLRPCServer.__init__(self, *args, **kwds)

		self.set_server_title('Brain XML RPC server')
		self.set_server_name('Brain XML RPC server methods')
		self.set_server_documentation("Keyword arguments should be passed in the last " +
			"parameter to function as a dictionary. If there are no keyword parameters, " +
			"empty dictionary should be passed.")

		# registering multicall function manually, because we should
		# remove keyword argument before calling it
		multicall = _KeywordFunction(self.system_multicall)
		multicall.__doc__ = "XML RPC multicall support"
		self.register_function(multicall, 'system.multicall')

	def register_instance(self, inst):
		DocXMLRPCServer.register_instance(self, _KeywordInstance(inst))


class _MyMultiCallMethod:
	"""Custom multicall method - adds keywords as the last function argument"""

	def __init__(self, call_list, name):
		self.__call_list = call_list
		self.__name = name

	def __getattr__(self, name):
		return _MyMultiCallMethod(self.__call_list, "%s.%s" % (self.__name, name))

	def __call__(self, *args, **kwds):
		args = tuple(list(args) + [kwds])
		self.__call_list.append((self.__name, args))


class _MyMultiCallIterator:
	"""Custom multicall iterator - raises known exceptions instead of Faults"""

	def __init__(self, results, exceptions):
		self._results = results
		self._exceptions = exceptions

	def __getitem__(self, i):
		item = self._results[i]
		if type(item) == type({}):
			_parseFault(item['faultCode'], item['faultString'], self._exceptions)
		elif type(item) == type([]):
			return item[0]
		else:
			raise ValueError("unexpected type in multicall result")


class MyMultiCall(MultiCall):
	"""Custom multicall object - adds session ID to all calls"""

	def __init__(self, server, session_id):
		self.__server = server
		self.__call_list = []
		self._session_id = session_id
		self._exceptions = self.__server._exceptions

	def __repr__(self):
		return "<MultiCall at %x>" % id(self)

	__str__ = __repr__

	def __getattr__(self, name):
		return _MyMultiCallMethod(self.__call_list, name)

	def __call__(self):
		marshalled_list = []
		for name, args in self.__call_list:
			args = tuple([self._session_id] + list(args))
			marshalled_list.append({'methodName' : name, 'params' : args})

		return _MyMultiCallIterator(self.__server.system.multicall(marshalled_list),
			self._exceptions)
