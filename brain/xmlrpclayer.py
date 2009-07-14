import threading
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Binary, Marshaller, Unmarshaller, Transport
import xmlrpc.client
import traceback
import random
import string
import re

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import brain
from brain import FacadeError, FormatError, LogicError, StructureError

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

class _MethodPy:
	def __init__(self, send, name):
		self.__send = send
		self.__name = name
	def __call__(self, *args, **kwargs):
		l = list(args)
		l.append(kwargs)
		args=tuple(l)
		return self.__send.__getattr__(self.__name)(*args)

class ServerProxyPy:
	def __init__(self, *args, **kwargs):
		self.s = ServerProxy(*args, **kwargs)
	def __getattr__(self, name):
		return _MethodPy(self.s, name)


class BrainXMLRPCError(brain.BrainError):
	pass

_CONNECTION_METHODS = ['create', 'modify', 'read', 'delete', 'insert',
	'readMany', 'insertMany', 'deleteMany', 'objectExists', 'search',
	'begin', 'beginSync', 'commit', 'rollback', 'close']

def _parse_result(res):
	return res

allowed_errors = [FacadeError, FormatError, LogicError, StructureError, BrainXMLRPCError]

error_pat = re.compile('(?P<exception>[^:]*):(?P<rest>.*$)')

class ExceptionUnmarshaller(xmlrpc.client.Unmarshaller):
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
				for exc in allowed_errors:
					if repr(exc) == exception_name:
						raise exc(rest)

			# Fall through and just raise the fault
			raise xmlrpc.client.Fault(**d)
		return tuple(self._stack)

class ExceptionTransport(xmlrpc.client.Transport):
	# Override user-agent if desired
	#user_agent = "xmlrpc-exceptions/0.0.1"

	def getparser(self, *args, **kwds):
		target = ExceptionUnmarshaller()
		parser = xmlrpc.client.ExpatParser(target)
		return parser, target


class _Dispatcher:

	def __init__(self):
		self._sessions = {}
		random.seed()

	def _dispatch(self, method, params):

		args=list(params)
		kwds=args.pop()
		args=tuple(args)

		if method in _CONNECTION_METHODS:
			return self._dispatch_connection_method(method, *args, **kwds)

		try:
			func = getattr(self, 'export_' + method)
		except AttributeError:
			raise BrainXMLRPCError('method ' + method + ' is not supported')
		else:
			return func(*args, **kwds)

	def _dispatch_connection_method(self, method, session_id, *args, **kwds):
		if not isinstance(session_id, str):
			raise BrainXMLRPCError("Session ID must be string")

		if session_id not in self._sessions:
			raise BrainXMLRPCError("Session " + str(session_id) + " does not exist")

		func = getattr(self._sessions[session_id], method)
		return func(*args, **kwds)

	def export_connect(self, path, open_existing, engine_tag):
		session_id = "".join(random.sample(string.ascii_letters + string.digits, 8))
		self._sessions[session_id] = brain.connect(path, open_existing, engine_tag)
		return session_id

	def export_close(self, session_id):
		self._sessions[session_id].close()
		del self._sessions[session_id]

	def export_getEngineTags(self):
		return brain.getEngineTags()

	def export_getDefaultEngineTag(self):
		return brain.getDefaultEngineTag()


class BrainServer:
	def __init__(self, port=8000, name=None):

		if name is None:
			name = "Brain XML RPC server on port " + str(port)

		self._server = SimpleXMLRPCServer(("localhost", port), allow_none=True,
			logRequests=False)
		self._server.allow_reuse_address = True
		self._server.register_instance(_Dispatcher())
		self._server_thread = threading.Thread(name=name,
			target=self._server.serve_forever)

	def start(self):
		self._server_thread.start()

	def stop(self):
		self._server.shutdown()
		self._server_thread.join()


class BrainClient:
	def __init__(self, addr):
		self._client = ServerProxyPy(addr, allow_none=True, transport=ExceptionTransport())

	def getEngineTags(self):
		return _parse_result(self._client.getEngineTags())

	def getDefaultEngineTag(self):
		return _parse_result(self._client.getDefaultEngineTag())

	def connect(self, path, open_existing=None, engine_tag=None):
		return _RemoteConnection(self._client, path, open_existing, engine_tag)


class _RemoteConnection:
	def __init__(self, client, path, open_existing, engine_tag):
		self._client = client
		self._session_id = _parse_result(client.connect(path, open_existing, engine_tag))

	def __getattr__(self, name):
		if name not in _CONNECTION_METHODS:
			raise AttributeError()

		method = getattr(self._client, name)
		def wrapper(*args, **kwds):
			return _parse_result(method(self._session_id, *args, **kwds))

		return wrapper
