"""
Facade for database - contains connect() and Connection class
"""

import functools
import inspect

from . import interface, logic, engine, op
from .interface import Field
from .decorator import decorator
from .data import *

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

def _isNotSearchCondition(arg):
	"""
	Check whether supplied argument looks like search condition.
	If it is a scalar, or a list of scalars - it is definitely not a condition.
	"""
	if not isinstance(arg, list):
		return True

	for elem in arg:
		if isinstance(elem, list):
			return False

	return True

def _getFirstSearchCondition(arg, position, engine):
	"""
	Find the first search condition in given list, starting from position,
	construct Condition object and return tuple (condition, new_position)
	"""

	# check if condition is inverted
	invert = False
	shift = position
	if arg[shift] == op.NOT:
		invert = True
		shift += 1

	# if nothing else is left, there is an error in condition
	if len(arg) == shift:
		raise interface.FormatError("Wrong number of elements in search condition")

	if _isNotSearchCondition(arg[shift]):
	# simple condition (field-comparison-value)

		if len(arg) - shift < 3:
			raise interface.FormatError("Wrong number of elements in search condition")

		value = Field(engine, [], arg[shift + 2])
		field = Field(engine, arg[shift], type_str=value.type_str)
		condition = interface.SearchRequest.Condition(field, arg[shift + 1], value, invert=invert)

		return condition, position + 3 + shift
	else:
	# complex condition
		condition = _listToSearchCondition(arg[shift], engine)
		if invert:
			condition.invert = not condition.invert

		return condition, 1 + shift

def _listToSearchCondition(arg, engine):
	"""Recursively transform list to Condition object"""

	if len(arg) == 0:
		return None

	# convolute long conditions starting from the beginning
	# (result will depend on calculation order)
	condition, position = _getFirstSearchCondition(arg, 0, engine)
	while position < len(arg):
		if len(arg) - position == 1:
			raise interface.FormatError("Wrong number of elements in search condition")
		operator = arg[position]
		next_condition, position = _getFirstSearchCondition(arg, position + 1, engine)
		condition = interface.SearchRequest.Condition(condition, operator,
			next_condition)

	return condition

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
			res.append(pathsToTree([(field.name, field.py_value) for field in result]))
		elif isinstance(request, interface.DumpRequest):
			for i, e in enumerate(result):
				# IDs have even indexes, lists of fields have odd ones
				if i % 2 == 1:
					result[i] = pathsToTree([(field.name, field.py_value) for field in result[i]])
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
					field.addNamePrefix(request.path.name)

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

		fields = [Field(self._engine, path, val) for path, val in treeToPaths(value)]
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
					mask.addNamePrefix(path.name)

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
			[[Field(self._engine, path, val) for path, val in treeToPaths(value)]
				for value in values],
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
		condition - [[NOT, ]condition, operator, condition] or
		[[NOT, ]field_name, operator, value]
		Returns list of object IDs.
		"""

		# syntax sugar: you may not wrap plain condition in a list
		if len(condition) != 1:
			condition = list(condition)

		condition_obj = _listToSearchCondition(condition, engine=self._engine)
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
		fields = [Field(self._engine, path, val) for path, val in treeToPaths(data)]

		for field in fields:
			field.addNamePrefix(path)

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
		Returns list [obj_id1, contents1, obj_id2, contents2, ...]
		"""
		self._requests.append(interface.DumpRequest())

	@_transacted
	def repair(self):
		"""Rebuild caching tables in database using existing contents."""
		self._requests.append(interface.RepairRequest())
