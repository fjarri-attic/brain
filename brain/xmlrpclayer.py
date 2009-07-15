import threading
import random
import string

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import brain
from brain import FacadeError, FormatError, LogicError, StructureError
from brain.xmlrpchelpers import MyXMLRPCServer, MyServerProxy

class BrainXMLRPCError(brain.BrainError):
	pass

_CONNECTION_METHODS = ['create', 'modify', 'read', 'delete', 'insert',
	'readMany', 'insertMany', 'deleteMany', 'objectExists', 'search',
	'begin', 'beginSync', 'commit', 'rollback', 'close']


class _Dispatcher:

	def __init__(self):
		self._sessions = {}
		random.seed()

	def _dispatch(self, method, *args, **kwds):

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

		self._server = MyXMLRPCServer(("localhost", port))
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
		self._client = MyServerProxy(addr, exceptions = [FacadeError,
			FormatError, LogicError, StructureError, BrainXMLRPCError])

	def getEngineTags(self):
		return self._client.getEngineTags()

	def getDefaultEngineTag(self):
		return self._client.getDefaultEngineTag()

	def connect(self, path, open_existing=None, engine_tag=None):
		return _RemoteConnection(self._client, path, open_existing, engine_tag)


class _RemoteConnection:
	def __init__(self, client, path, open_existing, engine_tag):
		self._client = client
		self._session_id = client.connect(path, open_existing, engine_tag)

	def __getattr__(self, name):
		if name not in _CONNECTION_METHODS:
			raise AttributeError()

		method = getattr(self._client, name)
		def wrapper(*args, **kwds):
			return method(self._session_id, *args, **kwds)

		return wrapper
