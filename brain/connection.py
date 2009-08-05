"""
Facade for database - contains connect() and Connection class
"""

import functools

from . import interface, logic, engine, op
from .interface import Field


def _flattenHierarchy(data, engine):
	"""Transform nested dictionaries and lists to a flat list of Field objects"""

	def flattenNode(node, prefix=[]):
		"""Transform current list/dictionary to a list of field name elements"""
		if isinstance(node, dict):
			results = [flattenNode(node[x], list(prefix) + [x]) for x in node.keys()]
			return functools.reduce(list.__add__, results, [])
		elif isinstance(node, list):
			results = [flattenNode(x, list(prefix) + [i]) for i, x in enumerate(node)]
			return functools.reduce(list.__add__, results, [])
		else:
			return [(prefix, node)]

	return [Field(engine, path, value) for path, value in flattenNode(data)]

def _saveTo(obj, ptr, path, value):
	"""Save given value to a place in hierarchy, defined by pointer"""

	# ensure that there is a place in obj where ptr points
	if isinstance(obj, list) and len(obj) < ptr + 1:
		# extend the list to corresponding index
		obj.extend([None] * (ptr + 1 - len(obj)))
	elif isinstance(obj, dict) and ptr not in obj:
		# create dictionary key
		obj[ptr] = None

	if len(path) == 0:
	# if we are in leaf now, store value
		obj[ptr] = value
	else:
	# if not, create required structure and call this function recursively
		if obj[ptr] is None:
			if isinstance(path[0], str):
				obj[ptr] = {}
			else:
				obj[ptr] = []

		_saveTo(obj[ptr], path[0], path[1:], value)

def _fieldsToTree(fields):
	"""Transform list of Field objects to nested dictionaries and lists"""

	if len(fields) == 0: return []

	# we need some starting object, whose pointer we can pass to recursive saveTo()
	res = []

	for field in fields:
		_saveTo(res, 0, field.name, field.value)

	# get rid of temporary root object and return only its first element
	return res[0]

def connect(engine_tag, *args, **kwds):
	"""Connect to database and return Connection object"""

	tags = engine.getEngineTags()
	if engine_tag is None: engine_tag = engine.getDefaultEngineTag()
	if engine_tag not in tags:
		raise interface.FacadeError("Unknown DB engine: " + str(engine_tag))

	engine_obj = engine.getEngineByTag(engine_tag)(*args, **kwds)

	return Connection(engine_obj)

def _tupleToSearchCondition(*args, engine):
	"""Transform tuple (path, operator, value) to Condition object"""

	# do not check whether the first argument is really NOT
	if len(args) == 3:
		invert = False
		shift = 0
	elif len(args) == 4:
		invert = True
		shift = 1
	elif len(args) == 0:
		return None
	else:
		raise interface.FormatError("Wrong number of elements in search condition")

	operand1 = args[shift]
	operand2 = args[2 + shift]
	operator = args[shift + 1]

	operand1 = (_tupleToSearchCondition(*operand1, engine=engine)
		if isinstance(operand1, tuple) else Field(engine, operand1))
	operand2 = (_tupleToSearchCondition(*operand2, engine=engine)
		if isinstance(operand2, tuple) else operand2)

	return interface.SearchRequest.Condition(operand1, args[1 + shift], operand2, invert)

def _transacted(func):
	"""Decorator for transacted methods of Connection"""

	def wrapper(obj, *args, **kwds):
		"""Function, which handles the transacted method"""

		if obj._sync:
		# synchronous transaction is currently in progress

			func(obj, *args, **kwds) # add request to list

			# try to process request, rollback on error
			try:
				handler, request = obj._prepareRequest(obj._requests[0])
				res = handler(request)
				processed = _transformResults(obj._requests, [res])
			except:
				obj.rollback()
				raise
			finally:
				obj._requests = []

			return processed[0]
		else:
		# no transaction or asynchronous transaction

			# if no transaction, create a new one
			create_transaction = not obj._transaction

			if create_transaction: obj.begin()
			func(obj, *args, **kwds)
			if create_transaction: return obj.commit()[0]

	return wrapper

def _propagateInversion(condition):
	"""Propagate inversion flags to the leafs of condition tree"""

	if not condition.leaf:
		if condition.invert:

			condition.invert = False

			# invert operands
			condition.operand1.invert = not condition.operand1.invert
			condition.operand2.invert = not condition.operand2.invert

			# invert operator
			if condition.operator == op.AND:
				condition.operator = op.OR
			elif condition.operator == op.OR:
				condition.operator = op.AND

		_propagateInversion(condition.operand1)
		_propagateInversion(condition.operand2)

def _transformResults(requests, results):
	"""Transform request results to a user-readable form"""
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
		elif isinstance(request, interface.ObjectExistsRequest):
			res.append(result)
		elif isinstance(request, interface.DumpRequest):
			for obj_id in result:
				result[obj_id] = _fieldsToTree(result[obj_id])
			res.append(result)

	return res


class Connection:
	"""Main control class of the database"""

	def __init__(self, engine):
		self._engine = engine
		self._logic = logic.LogicLayer(self._engine)

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
			interface.CreateRequest: self._logic.processCreateRequest,
			interface.ObjectExistsRequest: self._logic.processObjectExistsRequest,
			interface.DumpRequest: self._logic.processDumpRequest
		}

		# Prepare handler and request, if necessary
		# (so that we do not have to do it inside a transaction)
		if isinstance(request, interface.InsertRequest):

			# fields to insert have relative names
			for field_group in request.field_groups:
				for field in field_group:
					field.name = request.path.name + field.name

		elif isinstance(request, interface.SearchRequest) and \
				request.condition is not None:
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

	def close(self):
		"""Disconnect from database. All uncommitted changes can be lost."""
		self._engine.close()

	def begin(self):
		"""Begin asynchronous transaction"""
		if not self._transaction:
			self._transaction = True
			self._sync = False
		else:
			raise interface.FacadeError("Transaction is already in progress")

	def beginSync(self):
		"""Begin synchronous transaction"""
		if not self._transaction:
			self._engine.begin()
			self._transaction = True
			self._sync = True
		else:
			raise interface.FacadeError("Transaction is already in progress")

	def commit(self):
		"""Commit current transaction. Returns results in case of asynchronous transaction"""
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
				return _transformResults(self._requests, res)
			finally:
				self._requests = []

	def rollback(self):
		"""Rollback current transaction"""
		if not self._transaction:
			raise interface.FacadeError("Transaction is not in progress")

		self._transaction = False
		if self._sync:
			self._engine.rollback()
		else:
			self._requests = []

	@_transacted
	def modify(self, id, value, path=None):
		"""Create modification request and add it to queue"""
		if path is None and value is None: value = {}
		if path is None: path = []

		fields = _flattenHierarchy(value, self._engine)
		self._requests.append(interface.ModifyRequest(id, Field(self._engine, path), fields))

	def read(self, id, path=None):
		"""Create read request and add it to queue"""
		return self.readMany(id, [path] if path is not None else None)

	@_transacted
	def readMany(self, id, paths=None):
		"""Create multiple read request and add it to queue"""
		if paths is not None:
			paths = [Field(self._engine, path) for path in paths]
		self._requests.append(interface.ReadRequest(id, paths))

	def insert(self, id, path, value):
		"""Create insertion request and add it to queue"""
		return self.insertMany(id, path, [value])

	@_transacted
	def insertMany(self, id, path, values):
		"""Create several values insertion request and add it to queue"""
		self._requests.append(interface.InsertRequest(
			id, Field(self._engine, path),
			[_flattenHierarchy(value, self._engine) for value in values]))

	def delete(self, id, path=None):
		"""Create deletion request and add it to queue"""
		return self.deleteMany(id, [path] if path is not None else None)

	@_transacted
	def deleteMany(self, id, paths=None):
		"""Create many fields deletion request and add it to queue"""
		self._requests.append(interface.DeleteRequest(id,
			[Field(self._engine, path) for path in paths] if paths is not None else None
		))

	@_transacted
	def search(self, *args):
		"""Create search request and add it to queue"""
		condition = _tupleToSearchCondition(*args, engine=self._engine)
		self._requests.append(interface.SearchRequest(condition))

	@_transacted
	def create(self, data, path=None):
		"""Create creation request and add it to queue"""
		if path is None: path = []
		if data is not None:
			fields = _flattenHierarchy(data, self._engine)
		else:
			fields = []

		for field in fields:
			field.name = path + field.name

		self._requests.append(interface.CreateRequest(fields))

	@_transacted
	def objectExists(self, id):
		"""Create request which returns True if object with given ID exists"""
		self._requests.append(interface.ObjectExistsRequest(id))

	@_transacted
	def dump(self):
		"""Dump the whole database contents"""
		self._requests.append(interface.DumpRequest())


class FakeConnection:

	def __init__(self):
		self._id_counter = 0
		self._root = {}

	def create(self, data, path=None):
		if path is None: path = []
		self._id_counter += 1
		_saveTo(self._root, self._id_counter, path, data)
		return self._id_counter

	def modify(self, id, value, path=None):
		if path is None and value is None: pass
		if path is None: path = []
		_saveTo(self._root, id, path, value)

	def _getPath(self, obj, path):
		if len(path) == 1:
			return obj[path[0]]
		else:
			return self._getPath(obj[path[0]], path[1:])

	def insertMany(self, id, path, values):
		if id not in self._root:
			self._root[id] = {} if isinstance(path[0], str) else []

		target = self._getPath(self._root, [id] + path[:-1])
		index = path[-1]

		if index is None:
			for value in values:
				target.append(value)
		else:
			for value in values:
				target.insert(index, value)

	def _deleteAll(self, obj, path):
		if len(path) == 1:
			del obj[path[0]]
			return len(obj) == 0
		else:
			if path[0] is None:
				for x in obj:
					if self._deleteAll(obj[x], path[1:]) and (isinstance(obj, dict) or
							x == len(obj) - 1):
						del obj[x]
			else:
				if self._deleteAll(obj[path[0]], path[1:]) and (isinstance(obj, dict) or
						path[0] == len(obj) - 1):
					del obj[path[0]]

			return len(obj) == 0

	def deleteMany(self, id, paths=None):
		if paths is None:
			del self._root[id]
		else:
			for path in paths:
				self._deleteAll(self._root, [id] + path)

	def read(self, id):
		if id in self._root:
			return self._root[id]
		else:
			return None
