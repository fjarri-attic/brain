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

def connect(path, open_existing=None, engine_tag=None):

	if engine_tag is None: engine_tag = 'sqlite3'
	if engine_tag not in DB_ENGINES:
		raise interface.FacadeError("Unknown DB engine: " + str(engine_tag))

	engine = DB_ENGINES[engine_tag](path, open_existing)

	return Connection(engine)

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
				handler, request = obj._prepareRequest(obj._requests[0])
				res = handler(request)
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

def _propagateInversion(condition):
	"""Propagate inversion flags to the leafs of condition tree"""

	if not condition.leaf:
		if condition.invert:

			condition.invert = False

			condition.operand1.invert = not condition.operand1.invert
			condition.operand2.invert = not condition.operand2.invert

			if condition.operator == op.AND:
				condition.operator = op.OR
			elif condition.operator == op.OR:
				condition.operator = op.AND

		_propagateInversion(condition.operand1)
		_propagateInversion(condition.operand2)


class Connection:

	def __init__(self, engine):
		self._engine = engine
		structure = database.StructureLayer(self._engine)
		self._logic = database.LogicLayer(self._engine, structure)

		self._transaction = False
		self._sync = False
		self._requests = []

	def _prepareRequest(self, request):
		"""Prepare request for processing"""

		handlers = {
			interface.ModifyRequest: self._logic.processModifyRequest,
			interface.InsertRequest: self._logic.processInsertRequest,
			interface.ReadRequest: self._logic.processReadRequest,
			interface.DeleteRequest: self._logic.processDeleteRequest,
			interface.SearchRequest: self._logic.processSearchRequest,
			interface.CreateRequest: self._logic.processCreateRequest
		}

		# Prepare handler and request, if necessary
		# (so that we do not have to do it inside a transaction)
		if isinstance(request, interface.InsertRequest):

			# fields to insert have relative names
			for field_group in request.field_groups:
				for field in field_group:
					field.name = request.path.name + field.name

		elif isinstance(request, interface.SearchRequest):
			_propagateInversion(request.condition)

		return handlers[request.__class__], request

	def _processRequests(self, requests):
		"""Start/stop transaction, handle exceptions"""

		prepared_requests = [self._prepareRequest(x) for x in requests]

		# Handle request inside a transaction
		res = []
		self._engine.begin()
		try:
			for handler, request in prepared_requests:
				res.append(handler(request))
		except:
			self._engine.rollback()
			raise
		self._engine.commit()
		return res

	def disconnect(self):
		self._engine.disconnect()

	def begin(self):
		if not self._transaction:
			self._transaction = True
			self._sync = False
		else:
			raise interface.FacadeError("Transaction is already in progress")

	def begin_sync(self):
		if not self._transaction:
			self._engine.begin()
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
				self._engine.commit()
			finally:
				self._sync = False
		else:
			try:
				res = self._processRequests(self._requests)
				return self._transformResults(self._requests, res)
			finally:
				self._requests = []

	def rollback(self):

		if not self._transaction:
			raise interface.FacadeError("Transaction is not in progress")

		self._transaction = False
		if self._sync:
			self._engine.rollback()
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

		fields = _flattenHierarchy(value, self._engine)
		for field in fields:
			field.name = path + field.name
		self._requests.append(interface.ModifyRequest(id, fields))

	@_transacted
	def read(self, id, path=None):
		if path is not None:
			path = [Field(self._engine, path)]
		self._requests.append(interface.ReadRequest(id, path))

	@_transacted
	def insert(self, id, path, value):
		fields = _flattenHierarchy(value, self._engine)
		self._requests.append(interface.InsertRequest(
			id, Field(self._engine, path), [fields]))

	@_transacted
	def insert_many(self, id, path, values):
		self._requests.append(interface.InsertRequest(
			id, Field(self._engine, path),
			[_flattenHierarchy(value, self._engine) for value in values]))

	@_transacted
	def delete(self, id, path=None):
		self._requests.append(interface.DeleteRequest(id,
			[Field(self._engine, path)] if path is not None else None
		))

	@_transacted
	def search(self, *args):
		self._requests.append(interface.SearchRequest(
			_tupleToSearchCondition(*args, engine=self._engine)
		))

	@_transacted
	def create(self, data, path=None):
		if path is None: path = []
		if data is not None:
			fields = _flattenHierarchy(data, self._engine)
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

