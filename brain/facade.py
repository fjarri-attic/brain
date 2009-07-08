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

def flattenHierarchy(data, engine):
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

def fieldsToTree(fields):

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

def transacted(func):
	def handler(obj, *args, **kwds):

		if obj.sync:
			func(obj, *args, **kwds)
			try:
				res = obj.db.processRequestSync(obj.requests[0])
				processed = obj.transformResults(obj.requests, [res])
			except:
				obj.rollback()
				raise
			finally:
				obj.requests = []
			return processed[0]
		else:
			create_transaction = not obj.transaction
			if create_transaction: obj.begin()
			func(obj, *args, **kwds)
			if create_transaction:
				return obj.commit()[0]

	return handler


class Connection:

	def __init__(self, db):
		self.db = db
		self.transaction = False
		self.sync = False
		self.requests = []

	def disconnect(self):
		self.db.disconnect()

	def begin(self):
		if not self.transaction:
			self.transaction = True
			self.sync = False
		else:
			raise interface.FacadeError("Transaction is already in progress")

	def begin_sync(self):
		if not self.transaction:
			self.db.begin()
			self.transaction = True
			self.sync = True
		else:
			raise interface.FacadeError("Transaction is already in progress")

	def commit(self):

		if not self.transaction:
			raise interface.FacadeError("Transaction is not in progress")

		self.transaction = False
		if self.sync:
			try:
				self.db.commit()
			except:
				self.db.rollback()
				raise
			finally:
				self.sync = False
		else:
			try:
				res = self.db.processRequests(self.requests)
				return self.transformResults(self.requests, res)
			finally:
				self.requests = []

	def rollback(self):

		if not self.transaction:
			raise interface.FacadeError("Transaction is not in progress")

		self.transaction = False
		if self.sync:
			self.db.rollback()
		else:
			self.requests = []

	def transformResults(self, requests, results):
		res = []
		for result, request in zip(results, requests):
			if isinstance(request, interface.ReadRequest):
				res.append(fieldsToTree(result))
			elif isinstance(request, interface.ModifyRequest):
				res.append(result)
			elif isinstance(request, interface.InsertRequest):
				res.append(None)
			elif isinstance(request, interface.InsertManyRequest):
				res.append(None)
			elif isinstance(request, interface.DeleteRequest):
				res.append(None)
			elif isinstance(request, interface.SearchRequest):
				res.append(result)

		return res

	@transacted
	def modify(self, id, value, path=None):
		# FIXME: in case path and value are None, we should not poke the database at all
		if path is None and value is None: value = {}
		if path is None: path = []

		fields = flattenHierarchy(value, self.db.engine)
		for field in fields:
			field.name = path + field.name
		self.requests.append(interface.ModifyRequest(id, fields))

	@transacted
	def read(self, id, path=None):
		if path is not None:
			path = [Field(self.db.engine, path)]
		self.requests.append(interface.ReadRequest(id, path))

	@transacted
	def insert(self, id, path, value):
		fields = flattenHierarchy(value, self.db.engine)
		self.requests.append(interface.InsertRequest(
			id, Field(self.db.engine, path), fields))

	@transacted
	def insert_many(self, id, path, values):
		self.requests.append(interface.InsertManyRequest(
			id, Field(self.db.engine, path),
			[flattenHierarchy(value, self.db.engine) for value in values]))

	@transacted
	def delete(self, id, path=None):
		self.requests.append(interface.DeleteRequest(id,
			[Field(self.db.engine, path)] if path is not None else None
		))

	@transacted
	def _search(self, condition):
		self.requests.append(interface.SearchRequest(condition))

	def search(self, *args):
		return self._search(_tupleToSearchCondition(*args, engine=self.db.engine))

	def create(self, value, path=None):
		return self.modify(None, value, path)


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

