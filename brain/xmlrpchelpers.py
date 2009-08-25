"""
Helper classes for XML RPC used in this DB
As compared to original XML RPC, we add:
1) implicit bytes() marshalling/unmarshalling (so that user could pass such values
   without wrapping them in Binary())
2) tuples marshalling/unmarshalling
3) exceptions unmarshalling (will raise known exceptions on client side)

FIXME: this module contains several hacks which could cease to work after internal
Python library changes; moreover, they modify the behaviour of imported classes,
which can cause errors in other modules which use XML RPC (on client side, for example)

This is because Python xmlrpc package is designed in a way that actively prevents
users from adding abilities to it. The right way would be to rewrite xmlrpc, and it
will be possibly done in the future
"""

from xmlrpc.server import DocXMLRPCServer
from xmlrpc.client import ServerProxy, Binary, Marshaller, Unmarshaller, \
	Transport, MultiCall, MultiCallIterator
import xmlrpc.client
import re

def _transformBinary(data, back=False):

	actions = {
		dict: lambda x: {key: _transformBinary(x[key], back) for key in x},
		list: lambda x: [_transformBinary(elem, back) for elem in x],
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


class _ExceptionUnmarshaller(xmlrpc.client.Unmarshaller):
	"""Extension for Unmarshaller which will raise known exceptions on client side"""

	def __init__(self, exceptions, *args, **kwds):

		# known exception classes
		self._exceptions = exceptions

		# fault string for exception looks like "SomeError:error message goes here"
		self._error_pat = re.compile('(?P<exception>[^:]*):(?P<rest>.*$)')

		xmlrpc.client.Unmarshaller.__init__(self, *args, **kwds)

	def close(self):
		# return response tuple and target method
		if self._type is None or self._marks:
			raise xmlrpc.client.ResponseError()
		if self._type == "fault":
			d = self._stack[0]
			_parseFault(d['faultCode'], d['faultString'], self._exceptions)
		return tuple(self._stack)


class _ExceptionTransport(xmlrpc.client.Transport):
	"""Extension of default XML RPC transport"""

	# Override user-agent if desired
	#user_agent = "xmlrpc-exceptions/0.0.1"

	def __init__(self, exceptions):
		xmlrpc.client.Transport.__init__(self)
		self._exceptions = exceptions

	def getparser(self, *args, **kwds):
		target = _ExceptionUnmarshaller(self._exceptions)
		parser = xmlrpc.client.ExpatParser(target)
		return parser, target


class _KeywordMarshaller:
	"""Wrapper for method calls with keyword arguments, client side"""

	def __init__(self, send, name):
		self.__send = send
		self.__name = name

	def __call__(self, *args, **kwds):
		l = list(args)
		l = _transformBinary(l)
		l.append(kwds)
		args = tuple(l)
		res = self.__send.__getattr__(self.__name)(*args)
		res = _transformBinary(res, back=True)
		return res

	def __getattr__(self, name):
		return _KeywordMarshaller(self.__send, self.__name + '.' + name)


class MyServerProxy:
	"""Wrapper for XML RPC client, used for DB"""

	def __init__(self, *args, **kwds):
		if 'exceptions' in kwds:
			exceptions = kwds['exceptions']
			del kwds['exceptions']
		else:
			exceptions = []
		self._exceptions = exceptions

		kwds['transport'] = _ExceptionTransport(exceptions)
		kwds['allow_none'] = True

		self.s = ServerProxy(*args, **kwds)

	def __getattr__(self, name):
		return _KeywordMarshaller(self.s, name)


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
		args=list(args)
		kwds=args.pop()
		args=tuple(args)
		return self._func(*args, **kwds)


class MyXMLRPCServer(DocXMLRPCServer):
	"""Wrapper for XML RPC server, used for DB"""

	def __init__(self, *args, **kwds):
		kwds['allow_none'] = True
		kwds['logRequests'] = False
		DocXMLRPCServer.__init__(self, *args, **kwds)

		self.set_server_title('Brain XML RPC server')
		self.set_server_name('Brain XML RPC server methods')

		# registering multicall function manually, because we should
		# remove keyword argument before calling it
		multicall = _KeywordFunction(self.system_multicall)
		multicall.__doc__ = "XML RPC multicall support"
		self.register_function(multicall, 'system.multicall')

	def register_instance(self, inst):
		DocXMLRPCServer.register_instance(self, _KeywordInstance(inst))

class _MyMultiCallMethod:
	def __init__(self, call_list, name):
		self.__call_list = call_list
		self.__name = name
	def __getattr__(self, name):
		return _MyMultiCallMethod(self.__call_list, "%s.%s" % (self.__name, name))
	def __call__(self, *args, **kwds):
		args = tuple(list(args) + [kwds])
		self.__call_list.append((self.__name, args))


class _MyMultiCallIterator:
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
