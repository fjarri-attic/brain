from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Binary, Marshaller, Unmarshaller, Transport
import xmlrpc.client
import re

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

def dump_bytes(self, value, write):
	self.dump_instance(Binary(value), write)

def dump_tuple(self, value, write):
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

def start(self, tag, attrs):
	# prepare to handle this element
	if tag == "array" or tag == "struct" or tag == "tuple":
		self._marks.append(len(self._stack))
	self._data = []
	self._value = (tag == "value")

def end_base64(self, data):
	value = Binary()
	value.decode(data.encode("ascii"))
	self.append(value.data)
	self._value = 0

def end_tuple(self, data):
	mark = self._marks.pop()
	# map tuples to Python tuples
	self._stack[mark:] = [tuple(self._stack[mark:])]
	self._value = 0

xmlrpc.client.Marshaller.dispatch[tuple] = dump_tuple
xmlrpc.client.Marshaller.dispatch[bytes] = dump_bytes
xmlrpc.client.Unmarshaller.start = start
xmlrpc.client.Unmarshaller.dispatch["tuple"] = end_tuple
xmlrpc.client.Unmarshaller.dispatch["base64"] = end_base64

error_pat = re.compile('(?P<exception>[^:]*):(?P<rest>.*$)')

class ExceptionUnmarshaller(xmlrpc.client.Unmarshaller):

	def __init__(self, exceptions, *args, **kwds):
		self.exceptions = exceptions
		xmlrpc.client.Unmarshaller.__init__(self, *args, **kwds)

	def close(self):
		# return response tuple and target method
		if self._type is None or self._marks:
			raise xmlrpc.client.ResponseError()
		if self._type == "fault":
			d = self._stack[0]
			m = error_pat.match(d['faultString'])
			if m:
				exception_name = m.group('exception')
				rest = m.group('rest')
				for exc in self.exceptions:
					if repr(exc) == exception_name:
						raise exc(rest)

			# Fall through and just raise the fault
			raise xmlrpc.client.Fault(**d)
		return tuple(self._stack)

class ExceptionTransport(xmlrpc.client.Transport):
	# Override user-agent if desired
	#user_agent = "xmlrpc-exceptions/0.0.1"

	def __init__(self, exceptions):
		xmlrpc.client.Transport.__init__(self)
		self.exceptions = exceptions

	def getparser(self, *args, **kwds):
		target = ExceptionUnmarshaller(self.exceptions)
		parser = xmlrpc.client.ExpatParser(target)
		return parser, target

class _MethodPy:
	def __init__(self, send, name):
		self.__send = send
		self.__name = name
	def __call__(self, *args, **kwds):
		l = list(args)
		l.append(kwds)
		args=tuple(l)
		return self.__send.__getattr__(self.__name)(*args)

class MyServerProxy:
	def __init__(self, *args, **kwds):
		if 'exceptions' in kwds:
			exceptions = kwds['exceptions']
			del kwds['exceptions']
		else:
			exceptions = []

		kwds['transport'] = ExceptionTransport(exceptions)
		kwds['allow_none'] = True

		self.s = ServerProxy(*args, **kwds)
	def __getattr__(self, name):
		return _MethodPy(self.s, name)

class MyXMLRPCServer(SimpleXMLRPCServer):
	def __init__(self, *args, **kwds):
		kwds['allow_none'] = True
		kwds['logRequests'] = False
		SimpleXMLRPCServer.__init__(self, *args, **kwds)
