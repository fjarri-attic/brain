import threading
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import traceback
import random
import string

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import brain

def _error(error_msg, error_data=None):
	return {'error': error_msg, 'error_data': error_data, 'result': None}

def _result(data):
	return {'error': None, 'result': data}

def _catch(func, *args, **kwds):
	res = None
	try:
		res = func(*args, **kwds)
	except:
		exc_type, exc_val, exc_tb = sys.exc_info()
		exc_data = ''.join(traceback.format_exception(exc_type, exc_val, exc_tb))
		return _error(exc_val, exc_data)
	return _result(res)


class _Dispatcher:

	def __init__(self):
		self._sessions = {}
		random.seed()

	def _dispatch(self, method, params):

		connection_methods = ['create', 'modify', 'read', 'delete', 'insert',
			'read_many', 'insert_many', 'delete_many', 'object_exists', 'search']

		if method in connection_methods	:
			return self._dispatch_connection_method(method, *params)

		try:
			func = getattr(self, 'export_' + method)
		except AttributeError:
			return _error('method ' + method + ' is not supported')
		else:
			return _catch(func, *params)

	def _dispatch_connection_method(self, method, session_id, *args, **kwds):
		if not isinstance(session_id, str):
			return _error("Session ID must be string")

		if session_id not in self._sessions.keys():
			return _error("Session " + str(session_id) + " does not exist")

		return getattr(self._sessions[session_id], method)(*args, **kwds)

	def export_connect(self, path, open_existing, engine_tag):
		session_id = "".join(random.sample(string.ascii_letters + string.digits, 8))
		self._sessions[session_id] = brain.connect(path, open_existing, engine_tag)
		return session_id

	def export_close(self, session_id):
		self._sessions[session_id].close()
		del self._sessions[session_id]

	def export_getEngineTags(self):
		return brain.getEngineTags()


class BrainServer:
	def __init__(self, port=8000, name=None):

		if name is None:
			name = "Brain XML RPC server on port " + str(port)

		self._server = SimpleXMLRPCServer(("localhost", port), allow_none=True)
		self._server.register_instance(_Dispatcher())
		self._server_thread = threading.Thread(name=name,
			target=self._server.serve_forever)

	def start(self):
		self._server_thread.start()

	def stop(self):
		self._server.shutdown()
		self._server_thread.join()


print("main: creating server")
s = BrainServer()
print("main: starting server")
s.start()

print("main: creating client")
c = ServerProxy('http://localhost:8000', allow_none=True)
res = c.getEngineTags()
ses = c.connect(None, None, res['result'][0])
print(c.create(ses['result'], {'name': 'Alex'}))

print("main: shutting server down")
s.stop()
print("main: shutdown complete")
