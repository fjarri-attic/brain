import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

from brain import interface, database, engine
import brain.op as op
from brain.interface import Field
#import yaml
import functools

DB_ENGINES = {
	'sqlite3': engine.Sqlite3Engine
}

def _flattenHierarchy(data, engine):
	def flattenNode(node, prefix=[]):
		if isinstance(node, dict):
			results = [flattenNode(node[x], list(prefix) + [x]) for x in node.keys()]
			return functools.reduce(list.__add__, results, [])
		elif isinstance(node, list):
			results = [flattenNode(x, list(prefix) + [i]) for i, x in enumerate(node)]
			return functools.reduce(list.__add__, results, [])
		else:
			return [(prefix, node)]

	return [Field(engine, path, value) for path, value in flattenNode(data)]

def _fieldsToTree(fields):

	if len(fields) == 0: return []

	res = []

	def saveTo(obj, ptr, path, value):

		if isinstance(obj, list) and len(obj) < ptr + 1:
			obj.extend([None] * (ptr + 1 - len(obj)))
		elif isinstance(obj, dict) and ptr not in obj:
			obj[ptr] = None

		if len(path) == 0:
			obj[ptr] = value
		else:
			if obj[ptr] is None:
				if isinstance(path[0], str):
					obj[ptr] = {}
				else:
					obj[ptr] = []

			saveTo(obj[ptr], path[0], path[1:], value)

	for field in fields:
		saveTo(res, 0, field.name, field.value)

	return res[0]

def connect(path, open_existing=None, engine=None):

	if engine is None: engine = 'sqlite3'
	if engine not in DB_ENGINES:
		raise interface.FacadeError("Unknown DB engine: " + str(engine))

	return Connection(database.SimpleDatabase(
		DB_ENGINES[engine], path, open_existing))

def _tupleToSearchCondition(*args, engine):
	if len(args) == 4:
		invert = True
		shift = 1
	else:
		invert = False
		shift = 0

	operand1 = args[shift]
	operand2 = args[2 + shift]
	operator = args[shift + 1]

	operand1 = (_tupleToSearchCondition(*operand1, engine=engine) if isinstance(operand1, tuple) else Field(engine, operand1))
	operand2 = (_tupleToSearchCondition(*operand2, engine=engine) if isinstance(operand2, tuple) else operand2)

	return interface.SearchRequest.Condition(operand1, args[1 + shift], operand2, invert)

def _transacted(func):
	def handler(obj, *args, **kwds):

		if obj._sync:
			func(obj, *args, **kwds)
			try:
				res = obj._db.processRequestSync(obj._requests[0])
				processed = obj._transformResults(obj._requests, [res])
			except:
				obj.rollback()
				raise
			finally:
				obj._requests = []
			return processed[0]
		else:
			create_transaction = not obj._transaction
			if create_transaction: obj.begin()
			func(obj, *args, **kwds)
			if create_transaction:
				return obj.commit()[0]

	return handler


class Connection:

	def __init__(self, db):
		self._db = db
		self._transaction = False
		self._sync = False
		self._requests = []

	def disconnect(self):
		self._db.disconnect()

	def begin(self):
		if not self._transaction:
			self._transaction = True
			self._sync = False
		else:
			raise interface.FacadeError("Transaction is already in progress")

	def begin_sync(self):
		if not self._transaction:
			self._db.begin()
			self._transaction = True
			self._sync = True
		else:
			raise interface.FacadeError("Transaction is already in progress")

	def commit(self):

		if not self._transaction:
			raise interface.FacadeError("Transaction is not in progress")

		self._transaction = False
		if self._sync:
			try:
				self._db.commit()
			except:
				self._db.rollback()
				raise
			finally:
				self._sync = False
		else:
			try:
				res = self._db.processRequests(self._requests)
				return self._transformResults(self._requests, res)
			finally:
				self._requests = []

	def rollback(self):

		if not self._transaction:
			raise interface.FacadeError("Transaction is not in progress")

		self._transaction = False
		if self._sync:
			self._db.rollback()
		else:
			self._requests = []

	def _transformResults(self, requests, results):
		res = []
		for result, request in zip(results, requests):
			if isinstance(request, interface.ReadRequest):
				res.append(_fieldsToTree(result))
			elif isinstance(request, interface.ModifyRequest):
				res.append(None)
			elif isinstance(request, interface.InsertRequest):
				res.append(None)
			elif isinstance(request, interface.DeleteRequest):
				res.append(None)
			elif isinstance(request, interface.SearchRequest):
				res.append(result)
			elif isinstance(request, interface.CreateRequest):
				res.append(result)

		return res

	@_transacted
	def modify(self, id, value, path=None):
		if path is None and value is None: value = {}
		if path is None: path = []

		fields = _flattenHierarchy(value, self._db.engine)
		for field in fields:
			field.name = path + field.name
		self._requests.append(interface.ModifyRequest(id, fields))

	@_transacted
	def read(self, id, path=None):
		if path is not None:
			path = [Field(self._db.engine, path)]
		self._requests.append(interface.ReadRequest(id, path))

	@_transacted
	def insert(self, id, path, value):
		fields = _flattenHierarchy(value, self._db.engine)
		self._requests.append(interface.InsertRequest(
			id, Field(self._db.engine, path), [fields]))

	@_transacted
	def insert_many(self, id, path, values):
		self._requests.append(interface.InsertRequest(
			id, Field(self._db.engine, path),
			[_flattenHierarchy(value, self._db.engine) for value in values]))

	@_transacted
	def delete(self, id, path=None):
		self._requests.append(interface.DeleteRequest(id,
			[Field(self._db.engine, path)] if path is not None else None
		))

	@_transacted
	def search(self, *args):
		self._requests.append(interface.SearchRequest(
			_tupleToSearchCondition(*args, engine=self._db.engine)
		))

	@_transacted
	def create(self, data, path=None):
		if path is None: path = []
		if data is not None:
			fields = _flattenHierarchy(data, self._db.engine)
		else:
			fields = []

		for field in fields:
			field.name = path + field.name

		self._requests.append(interface.CreateRequest(fields))


class YamlFacade:

	def __init__(self, facade):
		self.facade = facade
		self.sessions = {}
		self.session_counter = 0

	def parseRequest(self, request):
		request = yaml.load(request)

		handlers = {
			'connect': self.processConnectRequest,
			'disconnect': self.processDisonnectRequest
		}

		if not 'type' in request:
			raise interface.FacadeError("Request type is missing")

		if not request['type'] in handlers:
			raise interface.FacadeError("Unknown request type: " + str(request['type']))

		return handlers[request['type']](request)

	def processConnectRequest(self, request):
		if not 'path' in request:
			raise interface.FacadeError('Database path is missing')
		path = request['path']

		open_existing = request['connect'] if 'connect' in request else None

		self.session_counter += 1
		self.sessions[self.session_counter] = self.facade.connect(path, open_existing)

		return self.session_counter

	def processDisonnectRequest(self, request):
		if not 'session' in request:
			raise interface.FacadeError('Session ID is missing')
		session = request['session']

		self.sessions[session].disconnect()
		del self.sessions[session]

