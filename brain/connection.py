"""
Facade for database - contains connect() and Connection class
"""

import functools
import inspect

from . import interface, logic, engine, op
from .interface import Field
from .decorator import decorator

def _flattenHierarchy(data, engine):
	"""Transform nested dictionaries and lists to a flat list of Field objects"""

	def flattenNode(node, prefix=[]):
		"""Transform current list/dictionary to a list of field name elements"""
		if isinstance(node, dict):
			results = [flattenNode(node[x], prefix + [x]) for x in node.keys()]
			return functools.reduce(list.__add__, results, [(prefix, dict())])
		elif isinstance(node, list):
			results = [flattenNode(x, prefix + [i]) for i, x in enumerate(node)]
			return functools.reduce(list.__add__, results, [(prefix, list())])
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
		if (value != [] and value != {}) or obj[ptr] is None:
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

	if len(fields) == 0:
		return []

	# we need some starting object, whose pointer we can pass to recursive saveTo()
	res = []

	for field in fields:
		_saveTo(res, 0, field.name, field.py_value)

	# get rid of temporary root object and return only its first element
	return res[0]

def connect(engine_tag, *args, remove_conflicts=False, **kwds):
	"""
	Connect to database.
	engine_tag - tag of engine which handles the database layer
	remove_conflicts - default setting of this parameter for modify() and insert()
	args and kwds - engine-specific parameters
	Returns Connection object for local connections or session ID for remote connections.
	"""
	tags = engine.getEngineTags()
	if engine_tag is None:
		engine_tag = engine.getDefaultEngineTag()
	if engine_tag not in tags:
		raise interface.FacadeError("Unknown DB engine: " + str(engine_tag))

	engine_class = engine.getEngineByTag(engine_tag)

	# leave only those keyword arguments, which are supported by engine
	argspec = inspect.getfullargspec(engine_class.__init__)
	engine_kwds = {}
	for key in kwds:
		if key in argspec.args:
			engine_kwds[key] = kwds[key]

	engine_obj = engine_class(*args, **engine_kwds)

	return Connection(engine_obj, remove_conflicts=remove_conflicts)

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
		if isinstance(operand2, tuple) else Field(engine, [], operand2))

	return interface.SearchRequest.Condition(operand1, args[1 + shift], operand2, invert)

@decorator
def _transacted(func, obj, *args, **kwds):
	"""Decorator for transacted methods of Connection"""

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

		if create_transaction: obj.beginAsync()

		try:
			func(obj, *args, **kwds)
		except:
			obj.rollback()
			raise

		if create_transaction:
			return obj.commit()[0]

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

	return_none = [interface.ModifyRequest, interface.InsertRequest,
	    interface.DeleteRequest, interface.RepairRequest]

	return_result = [interface.SearchRequest, interface.CreateRequest,
	    interface.ObjectExistsRequest]

	for result, request in zip(results, requests):
		request_type = type(request)
		if request_type in return_none:
			res.append(None)
		elif request_type in return_result:
			res.append(result)
		elif isinstance(request, interface.ReadRequest):
			res.append(_fieldsToTree(result))
		elif isinstance(request, interface.DumpRequest):
			for obj_id in result:
				result[obj_id] = _fieldsToTree(result[obj_id])
			res.append(result)

	return res


class Connection:
	"""Main control class of the database"""

	def __init__(self, engine, remove_conflicts=False):
		self._engine = engine
		self._logic = logic.LogicLayer(self._engine)

		self._transaction = False
		self._sync = False
		self._requests = []
		self._remove_conflicts = remove_conflicts

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
			interface.DumpRequest: self._logic.processDumpRequest,
			interface.RepairRequest: self._logic.processRepairRequest
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

		return handlers[type(request)], request

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
		"""
		Disconnect from database.
		All uncommitted changes will be lost.
		"""
		self._engine.close()

	def begin(self, sync):
		"""Begin synchronous or asynchronous transaction."""
		if not self._transaction:
			if sync:
				self._engine.begin()

			self._transaction = True
			self._sync = sync
		else:
			raise interface.FacadeError("Transaction is already in progress")

	def beginAsync(self):
		"""
		Begin asynchronous transaction.
		All request results will be returned during commit.
		"""
		self.begin(sync=False)

	def beginSync(self):
		"""
		Begin synchronous transaction.
		Each request result will be returned instantly.
		"""
		self.begin(sync=True)

	def commit(self):
		"""
		Commit current transaction.
		Returns results in case of asynchronous transaction.
		"""
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
	def modify(self, id, path, value, remove_conflicts=None):
		"""
		Modify existing object.
		id - object ID
		path - path to place where to save value
		value - data structure to save
		remove_conflicts - if True, remove all conflicting data structures;
			if False - taise an exception, if None - use connection default
		"""
		if path is None:
			path = []

		if remove_conflicts is None:
			remove_conflicts = self._remove_conflicts

		fields = _flattenHierarchy(value, self._engine)
		self._requests.append(interface.ModifyRequest(id,
			Field(self._engine, path), fields, remove_conflicts))

	@_transacted
	def read(self, id, path=None, masks=None):
		"""
		Read data structure from the object.
		id - object ID
		path - path to read from (root by default)
		masks - if specified, read only paths which start with one of given masks
		"""
		if masks is not None:
			masks = [Field(self._engine, mask) for mask in masks]

		if path is not None:
			path = Field(self._engine, path)
			if masks is not None:
				for mask in masks:
					mask.name = path.name + mask.name

		self._requests.append(interface.ReadRequest(id,
			path=path, masks=masks))

	def readByMask(self, id, mask=None):
		"""
		Read contents of existing object, filtered by mask.
		id - object ID
		mask - if specified, read only paths which start with this mask
		"""
		return self.read(id, path=None, masks=[mask] if mask is not None else None)

	def readByMasks(self, id, masks=None):
		"""
		Read contents of existing object, filtered by several masks.
		id - object ID
		masks - if specified, read only paths which start with one of given masks
		"""
		return self.read(id, path=None, masks=masks)

	def insert(self, id, path, value, remove_conflicts=None):
		"""
		Insert value into list.
		id - object ID
		path - path to insert to (must point to list)
		value - data structure to insert
		remove_conflicts - if True, remove all conflicting data structures;
			if False - taise an exception, if None - use connection default
		"""
		return self.insertMany(id, path, [value], remove_conflicts=remove_conflicts)

	@_transacted
	def insertMany(self, id, path, values, remove_conflicts=None):
		"""
		Insert several values into list.
		id - object ID
		path - path to insert to (must point to list)
		values - list of data structures to insert
		remove_conflicts - if True, remove all conflicting data structures;
			if False - taise an exception, if None - use connection default
		"""
		if remove_conflicts is None:
			remove_conflicts = self._remove_conflicts

		self._requests.append(interface.InsertRequest(
			id, Field(self._engine, path),
			[_flattenHierarchy(value, self._engine) for value in values],
			remove_conflicts))

	def delete(self, id, path=None):
		"""
		Delete existing object or its field.
		id - object ID
		path - path to value to delete (delete the whole object by default)
		"""
		return self.deleteMany(id, [path] if path is not None else None)

	@_transacted
	def deleteMany(self, id, paths=None):
		"""
		Delete existing object or some of its fields.
		id - object ID
		paths - list of paths to delete
		"""
		self._requests.append(interface.DeleteRequest(id,
			[Field(self._engine, path) for path in paths] if paths is not None else None
		))

	@_transacted
	def search(self, *condition):
		"""
		Search for object with specified fields.
		condition - ([NOT, ]condition, operator, condition) or
		([NOT, ]field_name, operator, value)
		Returns list of object IDs.
		"""
		condition_obj = _tupleToSearchCondition(*condition, engine=self._engine)
		self._requests.append(interface.SearchRequest(condition_obj))

	@_transacted
	def create(self, data, path=None):
		"""
		Create object with specified contents.
		data - initial contents
		path - path in object where to store these contents (root by default)
		Returns new object ID.
		"""
		if path is None:
			path = []
		fields = _flattenHierarchy(data, self._engine)

		for field in fields:
			field.name = path + field.name

		self._requests.append(interface.CreateRequest(fields))

	@_transacted
	def objectExists(self, id):
		"""
		Check whether object exists.
		id - object ID
		Returns True or False.
		"""
		self._requests.append(interface.ObjectExistsRequest(id))

	@_transacted
	def dump(self):
		"""
		Dump the whole database contents.
		Returns map of object IDs to object contents.
		"""
		self._requests.append(interface.DumpRequest())

	@_transacted
	def repair(self):
		"""Rebuild caching tables in database using existing contents."""
		self._requests.append(interface.RepairRequest())


class FakeConnection:
	"""
	Class which mimics some Connection methods, implementing them using Python data structures.
	It is used to test request results for real Connection class
	"""

	def __init__(self):
		self._id_counter = 0
		self._root = {}

	def _getPath(self, obj, path):
		"""Get pointer to data structure with given path"""
		if len(path) == 1:
			return obj[path[0]]
		else:
			return self._getPath(obj[path[0]], path[1:])

	def _deleteAll(self, obj, path):
		"""Delete all values from given path"""
		if len(path) == 1:
			del obj[path[0]]
		else:
			self._deleteAll(obj[path[0]], path[1:])

	def create(self, data, path=None):
		if path is None:
			path = []
		self._id_counter += 1
		_saveTo(self._root, self._id_counter, path, data)
		return self._id_counter

	def modify(self, id, path, value, remove_conflicts=False):
		_saveTo(self._root, id, path, value)

	def insertMany(self, id, path, values, remove_conflicts=False):
		target = self._getPath(self._root, [id] + path[:-1])
		index = path[-1]

		if index is None:
			for value in values:
				target.append(value)
		else:
			for value in reversed(values):
				target.insert(index, value)

	def deleteMany(self, id, paths=None):
		for path in paths:
			self._deleteAll(self._root, [id] + path)

	def read(self, id):
		return self._root[id]
