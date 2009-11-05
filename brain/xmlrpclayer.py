"""
XML RPC client and server for database
They use not exactly standard XML RPC, but slightly extended one;
see xmlrpchelpers.py for details
"""

import threading
import random
import string
import inspect

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import brain
from brain import FacadeError, FormatError, LogicError, StructureError
from brain.connection import TransactedConnection, TRANSACTED_METHODS
from brain.xmlrpchelpers import MyXMLRPCServer, MyServerProxy, MyMultiCall

class BrainXMLRPCError(brain.BrainError):
	"""Signals an error in XML RPC layer"""
	pass

# methods, calls to which will be forwared to connection object
_PURE_METHODS = ['begin', 'beginSync', 'beginAsync', 'commit', 'rollback']
_CONNECTION_METHODS = _PURE_METHODS + TRANSACTED_METHODS


class _Dispatcher:
	"""Handles remote calls on server side, creates sessions and connection objects"""

	def __init__(self, db_path=None):
		self._sessions = {}
		self._db_path = db_path
		random.seed()

	def _dispatch(self, method, *args, **kwds):
		"""Handle remote call; pass it to this object or to connection object"""

		if method in _CONNECTION_METHODS:
			return self._dispatch_connection_method(method, *args, **kwds)

		try:
			func = getattr(self, 'export_' + method)
		except AttributeError:
			raise BrainXMLRPCError('method ' + method + ' is not supported')
		else:
			return func(*args, **kwds)

	def _dispatch_connection_method(self, method, session_id, *args, **kwds):
		"""Pass connection method to corresponding session"""

		if not isinstance(session_id, str):
			raise BrainXMLRPCError("Session ID must be string")

		if session_id not in self._sessions:
			raise BrainXMLRPCError("Session " + str(session_id) + " does not exist")

		func = getattr(self._sessions[session_id], method)
		return func(*args, **kwds)

	def export_connect(self, *args, **kwds):
		session_id = "".join(random.sample(string.ascii_letters + string.digits, 8))
		if self._db_path is not None:
			kwds['db_path'] = self._db_path
		self._sessions[session_id] = brain.connect(*args, **kwds)
		return session_id

	def export_close(self, session_id):
		self._sessions[session_id].close()
		del self._sessions[session_id]

	def export_getEngineTags(self):
		return brain.getEngineTags()

	def export_getDefaultEngineTag(self):
		return brain.getDefaultEngineTag()

	def _listMethods(self):
		"""Helper for documenting XML RPC server - returns list of instance method"""
		return _CONNECTION_METHODS + ['connect', 'close',
			'getEngineTags', 'getDefaultEngineTag']

	def _findFunction(self, method_name):
		"""Returns instance function object by name"""

		func = None
		if method_name in TRANSACTED_METHODS:
			func = getattr(brain.connection.Connection, "_prepare_" + method_name)
		elif method_name in _PURE_METHODS + ['close']:
			func = getattr(brain.connection.Connection, method_name)
		elif method_name == 'connect':
			func = brain.connect
		elif method_name in ['getEngineTags', 'getDefaultEngineTag']:
			func = getattr(brain.engine, method_name)
		return func

	def _constructEnginesList(self):
		"""Returns the description of constructor arguments for all available engines"""

		tags = brain.getEngineTags()
		default_tag = brain.getDefaultEngineTag()

		res = ""
		for tag in tags:
			engine_class = brain.engine.getEngineByTag(tag)
			arg_spec = tuple(inspect.getfullargspec(engine_class.__init__))
			arg_str = inspect.formatargspec(*arg_spec)
			default = " (default)" if tag == default_tag else ""
			res += tag + default + ": " + arg_str + "\n"
		return res

	def _methodHelp(self, method_name):
		"""Helper for documenting XML RPC server - returns method help by name"""

		func = self._findFunction(method_name)

		if func is None:
			return None

		func_help = inspect.getdoc(func)

		if method_name == "getEngineTags":
			return func_help + "\nEngine constructors:\n" + self._constructEnginesList()
		else:
			return func_help

	def _get_method_argstring(self, method_name):
		"""Helper for documenting XML RPC server - returns method argstring by name"""

		func = self._findFunction(method_name)

		if func is None:
			return None

		arg_spec = tuple(inspect.getfullargspec(func))

		# replace first argument ('self', because they are the methods
		# of Connection class) by 'session_id'
		if method_name in _CONNECTION_METHODS + ['close']:
			arg_spec[0][0] = 'session_id'

		return inspect.formatargspec(*arg_spec)


class Server:
	"""Class for brain DB server"""

	def __init__(self, port=8000, name=None, db_path=None):

		# server thread name
		if name is None:
			name = "Brain XML RPC server on port " + str(port)

		self._server = MyXMLRPCServer(("localhost", port))
		self._server.allow_reuse_address = True
		self._server.register_instance(_Dispatcher(db_path=db_path))
		self._server_thread = threading.Thread(name=name,
			target=self._server.serve_forever)

	def start(self):
		"""Start server"""
		self._server_thread.start()

	def stop(self):
		"""Stop server and wait for it to finish"""
		self._server.shutdown()
		self._server_thread.join()


class Client:
	"""Class for brain DB client"""

	def __init__(self, addr):
		self._client = MyServerProxy(addr, exceptions=[FacadeError,
			FormatError, LogicError, StructureError, BrainXMLRPCError])

	def getEngineTags(self):
		return self._client.getEngineTags()

	def getDefaultEngineTag(self):
		return self._client.getDefaultEngineTag()

	def connect(self, *args, **kwds):
		return _RemoteConnection(self._client, *args, **kwds)


class _RemoteConnection(TransactedConnection):
	"""Class which mimics local DB connection"""

	def __init__(self, client, *args, remove_conflicts=False, **kwds):
		TransactedConnection.__init__(self)
		self._client = client
		self._remove_conflicts = remove_conflicts
		self._session_id = client.connect(*args, remove_conflicts=remove_conflicts, **kwds)
		self._multicall = None

	def getRemoveConflicts(self):
		return self._remove_conflicts

	def _handleRequests(self, requests):

		if self._multicall is None:
			res = []
			for name, args, kwds in requests:
				result = getattr(self._client, name)(self._session_id, *args, **kwds)
				res.append(result)
			return res
		else:
			for name, args, kwds in requests:
				getattr(self._multicall, name)(*args, **kwds)
			return list(self._multicall())[-1]

	def _begin(self, sync):
		if sync:
			self._multicall = None
			self._client.beginSync(self._session_id)
		else:
			self._multicall = MyMultiCall(self._client, self._session_id)

	def _rollback(self):
		self._client.rollback(self._session_id)

	def _commit(self):
		self._client.commit(self._session_id)

	def close(self):
		self._client.close(self._session_id)
