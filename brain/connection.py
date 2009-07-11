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

def _fieldsToTree(fields):
	"""Transform list of Field objects to nested dictionaries and lists"""

	if len(fields) == 0: return []

	def saveTo(obj, ptr, path, value):
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

			saveTo(obj[ptr], path[0], path[1:], value)

	# we need some starting object, whose pointer we can pass to recursive saveTo()
	res = []

	for field in fields:
		saveTo(res, 0, field.name, field.value)

	# get rid of temporary root object and return only its first element
	return res[0]

def connect(path, open_existing=None, engine_tag=None):
	"""Connect to database and return Connection object"""

	tags = engine.getEngineTags()
	if engine_tag is None: engine_tag = tags[0]
	if engine_tag not in tags:
		raise interface.FacadeError("Unknown DB engine: " + str(engine_tag))

	engine_obj = engine.getEngineByTag(engine_tag)(path, open_existing)

	return Connection(engine_obj)

def _tupleToSearchCondition(*args, engine):
	"""Transform tuple (path, operator, value) to Condition object"""

	# do not check whether the first argument is really NOT
	if len(args) == 4:
		invert = True
		shift = 1
	else:
		invert = False
		shift = 0

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

	def begin_sync(self):
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
		for field in fields:
			field.name = path + field.name
		self._requests.append(interface.ModifyRequest(id, fields))

	def read(self, id, path=None):
		"""Create read request and add it to queue"""
		return self.read_many(id, [path] if path is not None else None)

	@_transacted
	def read_many(self, id, paths=None):
		"""Create multiple read request and add it to queue"""
		if paths is not None:
			paths = [Field(self._engine, path) for path in paths]
		self._requests.append(interface.ReadRequest(id, paths))

	def insert(self, id, path, value):
		"""Create insertion request and add it to queue"""
		return self.insert_many(id, path, [value])

	@_transacted
	def insert_many(self, id, path, values):
		"""Create several values insertion request and add it to queue"""
		self._requests.append(interface.InsertRequest(
			id, Field(self._engine, path),
			[_flattenHierarchy(value, self._engine) for value in values]))

	def delete(self, id, path=None):
		"""Create deletion request and add it to queue"""
		return self.delete_many(id, [path] if path is not None else None)

	@_transacted
	def delete_many(self, id, paths=None):
		"""Create many fields deletion request and add it to queue"""
		self._requests.append(interface.DeleteRequest(id,
			[Field(self._engine, path) for path in paths] if paths is not None else None
		))

	@_transacted
	def search(self, *args):
		"""Create search request and add it to queue"""
		self._requests.append(interface.SearchRequest(
			_tupleToSearchCondition(*args, engine=self._engine)
		))

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
