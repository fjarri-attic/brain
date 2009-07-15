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

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Binary, Marshaller, Unmarshaller, Transport
import xmlrpc.client
import re

# Additional marshalling functions

def _dump_bytes(self, value, write):
	self.dump_instance(Binary(value), write)

def _dump_tuple(self, value, write):
	i = id(value)
	if i in self.memo:
		raise TypeError("cannot marshal recursive sequences")
	self.memo[i] = None
	dump = self._Marshaller__dump
	write("<value><tuple><data>\n")
	for v in value:
		dump(v, write)
	write("</data></tuple></value>\n")
	del self.memo[i]

# Additional unmarshalling functions

def _start(self, tag, attrs):
	# prepare to handle this element
	if tag == "array" or tag == "struct" or tag == "tuple":
		self._marks.append(len(self._stack))
	self._data = []
	self._value = (tag == "value")

def _end_base64(self, data):
	value = Binary()
	value.decode(data.encode("ascii"))
	self.append(value.data)
	self._value = 0

def _end_tuple(self, data):
	mark = self._marks.pop()
	# map tuples to Python tuples
	self._stack[mark:] = [tuple(self._stack[mark:])]
	self._value = 0

# Alter the behaviour of loaded Marshaller and Unmarshaller
xmlrpc.client.Marshaller.dispatch[tuple] = _dump_tuple
xmlrpc.client.Marshaller.dispatch[bytes] = _dump_bytes
xmlrpc.client.Unmarshaller.start = _start
xmlrpc.client.Unmarshaller.dispatch["tuple"] = _end_tuple
xmlrpc.client.Unmarshaller.dispatch["base64"] = _end_base64


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
			m = self._error_pat.match(d['faultString'])
			if m:
				exception_name = m.group('exception')
				rest = m.group('rest')
				for exc in self._exceptions:
					if repr(exc) == exception_name:
						raise exc(rest)

			# Fall through and just raise the fault
			raise xmlrpc.client.Fault(**d)
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
		l.append(kwds)
		args=tuple(l)
		return self.__send.__getattr__(self.__name)(*args)


class MyServerProxy:
	"""Wrapper for XML RPC client, used for DB"""

	def __init__(self, *args, **kwds):
		if 'exceptions' in kwds:
			exceptions = kwds['exceptions']
			del kwds['exceptions']
		else:
			exceptions = []

		kwds['transport'] = _ExceptionTransport(exceptions)
		kwds['allow_none'] = True

		self.s = ServerProxy(*args, **kwds)

	def __getattr__(self, name):
		return _KeywordMarshaller(self.s, name)


class _KeywordUnmarshaller:
	"""Wrapper for method calls with keyword arguments, server side"""

	def __init__(self, inst):
		self._inst = inst

	def _dispatch(self, method, params):

		args=list(params)
		kwds=args.pop()
		args=tuple(args)

		return self._inst._dispatch(method, *args, **kwds)


class MyXMLRPCServer(SimpleXMLRPCServer):
	"""Wrapper for XML RPC server, used for DB"""

	def __init__(self, *args, **kwds):
		kwds['allow_none'] = True
		kwds['logRequests'] = False
		SimpleXMLRPCServer.__init__(self, *args, **kwds)

	def register_instance(self, inst):
		SimpleXMLRPCServer.register_instance(self, _KeywordUnmarshaller(inst))
