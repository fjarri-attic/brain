"""
Facade for database - contains connect() and Connection class
"""

import functools
import inspect

from . import interface, logic, engine, op
from .interface import Field
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


class TransactedConnection:

	def __init__(self):
		self.__transaction = False
		self.__sync = False
		self.__requests = []

	def begin(self, sync):
		"""Begin synchronous or asynchronous transaction."""

		if self.__transaction:
			raise interface.FacadeError("Transaction is already in progress")

		if sync:
			self._begin()

		self.__transaction = True
		self.__sync = sync

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

	def __prepareRequest(self, name, *args, **kwds):
		return self.__getattribute__("_prepare_" + name)(*args, **kwds)

	def __processResult(self, name, result):
		try:
			return self.__getattribute__("_process_" + name)(result)
		except AttributeError:
			return result

	def commit(self):
		"""
		Commit current transaction.
		Returns results in case of asynchronous transaction.
		"""
		if not self.__transaction:
			raise interface.FacadeError("Transaction is not in progress")

		self.__transaction = False
		if self.__sync:
			try:
				self._commit()
			finally:
				self.__sync = False
		else:
			try:
				prepared_requests = []
				names = []
				for name, args, kwds in self.__requests:
					names.append(name)
					prepared_requests.append(self.__prepareRequest(
						name, *args, **kwds))

				results = self._handleRequests(prepared_requests)

				return [self.__processResult(name, result) for name, result
					in zip(names, results)]
			except:
				self._rollback()
				raise
			finally:
				self.__requests = []

	def rollback(self):
		"""Rollback current transaction"""
		if not self.__transaction:
			raise interface.FacadeError("Transaction is not in progress")

		self.__transaction = False
		if self.__sync:
			self._rollback()
		else:
			self.__requests = []

	def __transacted(self, name, *args, **kwds):

		if self.__sync:
		# synchronous transaction is currently in progress

			# try to process request, rollback on error
			try:
				prepared_request = self.__prepareRequest(name, *args, **kwds)
				results = self._handleRequests([prepared_request])
				processed = self.__processResult(name, results[0])
			except:
				self.rollback()
				raise

			return processed
		else:
		# no transaction or asynchronous transaction

			# if no transaction, create a new one
			create_transaction = not self.__transaction

			if create_transaction:
				self.beginAsync()
			self.__requests.append((name, args, kwds))
			if create_transaction:
				return self.commit()[0]

	def __getattr__(self, name):
		def handler(*args, **kwds):
			return self.__transacted(name, *args, **kwds)

		return handler

class Connection(TransactedConnection):
	"""Main control class of the database"""

	def __init__(self, engine, remove_conflicts=False):
		TransactedConnection.__init__(self)
		self._engine = engine
		self._logic = logic.LogicLayer(self._engine)
		self._remove_conflicts = remove_conflicts

	def _begin(self):
		self._engine.begin()

	def _commit(self):
		self._engine.commit()

	def _rollback(self):
		self._engine.rollback()

	def _handleRequests(self, requests):
		"""Start/stop transaction, handle exceptions"""

		# Handle request inside a transaction
		res = []
		for handler, handler_args, handler_kwds in requests:
			res.append(handler(*handler_args, **handler_kwds))
		return res

	def _process_read(self, result):
		return pathsToTree([(field.name, field.py_value) for field in result])

	def _process_readByMask(self, result):
		return self._process_read(result)

	def _process_readByMasks(self, result):
		return self._process_read(result)

	def _process_dump(self, result):
		for i, e in enumerate(result):
			# IDs have even indexes, lists of fields have odd ones
			if i % 2 == 1:
				result[i] = pathsToTree([(field.name, field.py_value) for field in result[i]])
		return result

	def close(self):
		"""
		Disconnect from database.
		All uncommitted changes will be lost.
		"""
		self._engine.close()

	def _prepare_modify(self, id, path, value, remove_conflicts=None):
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
		return self._logic.processModifyRequest, \
			(interface.ModifyRequest(id, Field(self._engine, path), fields, remove_conflicts),), {}

	def _prepare_read(self, id, path=None, masks=None):
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

		return self._logic.processReadRequest, (interface.ReadRequest(id,
			path=path, masks=masks),), {}

	def _prepare_readByMask(self, id, mask=None):
		"""
		Read contents of existing object, filtered by mask.
		id - object ID
		mask - if specified, read only paths which start with this mask
		"""
		return self._prepare_read(id, path=None, masks=[mask] if mask is not None else None)

	def _prepare_readByMasks(self, id, masks=None):
		"""
		Read contents of existing object, filtered by several masks.
		id - object ID
		masks - if specified, read only paths which start with one of given masks
		"""
		return self._prepare_read(id, path=None, masks=masks)

	def _prepare_insert(self, id, path, value, remove_conflicts=None):
		"""
		Insert value into list.
		id - object ID
		path - path to insert to (must point to list)
		value - data structure to insert
		remove_conflicts - if True, remove all conflicting data structures;
			if False - taise an exception, if None - use connection default
		"""
		return self._prepare_insertMany(id, path, [value], remove_conflicts=remove_conflicts)

	def _prepare_insertMany(self, id, path, values, remove_conflicts=None):
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

		request = interface.InsertRequest(
			id, Field(self._engine, path),
			[[Field(self._engine, path, val) for path, val in treeToPaths(value)]
				for value in values],
			remove_conflicts)

		for field_group in request.field_groups:
			for field in field_group:
				field.addNamePrefix(request.path.name)

		return self._logic.processInsertRequest, (request,), {}

	def _prepare_delete(self, id, path=None):
		"""
		Delete existing object or its field.
		id - object ID
		path - path to value to delete (delete the whole object by default)
		"""
		return self._prepare_deleteMany(id, [path] if path is not None else None)

	def _prepare_deleteMany(self, id, paths=None):
		"""
		Delete existing object or some of its fields.
		id - object ID
		paths - list of paths to delete
		"""
		return self._logic.processDeleteRequest, (interface.DeleteRequest(id,
			[Field(self._engine, path) for path in paths] if paths is not None else None
		),), {}

	def _prepare_search(self, *condition):
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
		request = interface.SearchRequest(condition_obj)

		if request.condition is not None:
			_propagateInversion(request.condition)

		return self._logic.processSearchRequest, (request,), {}

	def _prepare_create(self, data, path=None):
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

		return self._logic.processCreateRequest, (interface.CreateRequest(fields),), {}

	def _prepare_objectExists(self, id):
		"""
		Check whether object exists.
		id - object ID
		Returns True or False.
		"""
		return self._logic.processObjectExistsRequest, (interface.ObjectExistsRequest(id),), {}

	def _prepare_dump(self):
		"""
		Dump the whole database contents.
		Returns list [obj_id1, contents1, obj_id2, contents2, ...]
		"""
		return self._logic.processDumpRequest, (interface.DumpRequest(),), {}

	def _prepare_repair(self):
		"""Rebuild caching tables in database using existing contents."""
		return self._logic.processRepairRequest, (interface.RepairRequest(),), {}
