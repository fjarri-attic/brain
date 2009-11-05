"""
Facade for database - contains connect() and Connection class
"""

import copy
import inspect

from . import interface, logic, engine, op
from .interface import Field
from .data import *

# methods, which should be handled using the transaction logic
TRANSACTED_METHODS = ['create', 'modify', 'read', 'delete', 'insert',
	'readByMask', 'readByMasks', 'insertMany', 'deleteMany', 'objectExists', 'search',
	'dump', 'repair']

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
	"""
	Class which implemets basic transaction logic
	(including special logic for synchronous and asynchronous transactions).
	"""

	def __init__(self):
		self.__transaction = False
		self.__sync = False
		self.__requests = []

	def _sync(self):
		return self.__sync

	def begin(self, sync):
		"""Begin synchronous or asynchronous transaction."""

		if self.__transaction:
			raise interface.FacadeError("Transaction is already in progress")

		self._begin(sync)

		self.__requests = []
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

	def _prepareRequest(self, name, *args, **kwds):
		"""
		Default function which is called in order to prepare transacted request
		parameters for processing. By default it tries to call specific method,
		and if it is not found, it returns initial arguments.
		"""
		try:
			return self.__getattribute__("_prepare_" + name)(*args, **kwds)
		except AttributeError:
			return args, kwds

	def _processResult(self, name, result):
		"""
		Default function which is called in order to process transacted request
		result. By default it tries to call specific method,
		and if it is not found, it returns untouched result.
		"""
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
		# Synchronous transaction - just try to commit whatever was done
			self._commit()
		else:
		# Asynchronous transaction
			prepared_requests = []
			names = []

			# Add asynchronous begin and commit to requests
			# (so that they are processed in a single function
			# with other requests)
			prepared_begin_args, prepared_begin_kwds = self._prepareRequest('begin', sync=False)
			prepared_commit_args, prepared_commit_kwds = self._prepareRequest('commit')

			requests = [('begin', prepared_begin_args, prepared_begin_kwds)] + \
				self.__requests + \
				[('commit', prepared_commit_args, prepared_commit_kwds)]

			try:
				# prepare request parameters
				for name, args, kwds in requests:
					names.append(name)
					prepared_requests.append((name, args, kwds))

				# process all requests, from begin to commit in a single function
				# (derived class can have an ability to process several requests
				# faster, if it knows that they are successive)
				results = self._handleRequests(prepared_requests)
			except:
				# give derived class a chance to clean up failed transaction
				self._onError()
				raise

			# return processed results (except for results of begin and commit,
			# which do not return anything - because the begin() was asynchronous)
			return [self._processResult(name, result) for name, result
				in zip(names[1:-1], results)]

	def rollback(self):
		"""Rollback current transaction"""
		if not self.__transaction:
			raise interface.FacadeError("Transaction is not in progress")

		self.__transaction = False
		if self.__sync:
			self._rollback()

	def _onError(self):
		"""Default transaction error handler"""
		self.__transaction = False

	def close(self):
		"""
		Disconnect from database.
		All uncommitted changes will be lost.
		"""
		self._close()

	def __transacted(self, name, *args, **kwds):
		"""Transacted method handler"""

		if not self.__transaction:
		# if transaction is not started, start the asynchronous transaction
			self.beginAsync()
			getattr(self, name)(*args, **kwds)
			return self.commit()[0]

		# prepare arguments in place, just in case _prepareRequest
		# throws an error (it can happen)
		try:
			prepared_args, prepared_kwds = self._prepareRequest(name, *args, **kwds)
		except:
			# give derived class a chance to clean up failed transaction
			self._onError()
			raise

		if self.__sync:
		# synchronous transaction is currently in progress

			try:
				results = self._handleRequests([(name, prepared_args, prepared_kwds)])
				processed = self._processResult(name, results[0])
			except:
				# give derived class a chance to clean up failed transaction
				self._onError()
				raise

			return processed

		else:
		# asynchronous transaction is currently in progress
			self.__requests.append((name, prepared_args, prepared_kwds))

	def __getattr__(self, name):
		"""Transacted methods handlers generator"""

		if name not in TRANSACTED_METHODS:
			raise AttributeError("Unknown transacted method: " + name)

		return lambda *args, **kwds: self.__transacted(name, *args, **kwds)


class Connection(TransactedConnection):
	"""Main control class of the database"""

	def __init__(self, engine, remove_conflicts=False):
		TransactedConnection.__init__(self)
		self._engine = engine
		self._logic = logic.LogicLayer(self._engine)
		self._remove_conflicts = remove_conflicts

		# Since this class handles asynchronous transaction as if it is
		# a synchronous one, it has to look for begin/end of transactions
		# itself
		self._transaction = False

		self._handlers = {
			'commit': self._engine.commit,
			'begin': self._manual_begin,
			'create': self._logic.processCreateRequest,
			'read': self._logic.processReadRequest,
			'readByMask': self._logic.processReadRequest,
			'readByMasks': self._logic.processReadRequest,
			'delete': self._logic.processDeleteRequest,
			'deleteMany': self._logic.processDeleteRequest,
			'search': self._logic.processSearchRequest,
			'modify': self._logic.processModifyRequest,
			'insert': self._logic.processInsertRequest,
			'insertMany': self._logic.processInsertRequest,
			'objectExists': self._logic.processObjectExistsRequest,
			'dump': self._logic.processDumpRequest,
			'repair': self._logic.processRepairRequest
		}

	def getRemoveConflicts(self):
		"""Get current default value of remove_conflicts keyword."""
		return self._remove_conflicts

	def _engine_begin(self):
		self._engine.begin()
		self._transaction = True

	def _begin(self, sync):
		if sync:
			self._engine_begin()

	def _manual_begin(self, sync):
		self._engine_begin()

	def _commit(self):
		self._transaction = False
		self._engine.commit()

	def _rollback(self):
		self._transaction = False
		self._engine.rollback()

	def _onError(self):
		if self._transaction:
			self._rollback()
		TransactedConnection._onError(self)

	def _handleRequests(self, requests):
		"""Start/stop transaction, handle exceptions"""

		# TODO: probably it will be faster to pass the whole request
		# package to the underlying layer.
		# Currently we handle asynchronous and synchronous transaction
		# similarly - just calling corresponding Logic class methods
		res = []
		for name, args, kwds in requests:
			res.append(self._handlers[name](*args, **kwds))
		if self._sync():
			return res
		else:
			return res[1:-1]

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

	def _close(self):
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
		return (interface.ModifyRequest(id, Field(self._engine, path), fields, remove_conflicts),), {}

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

		return (interface.ReadRequest(id, path=path, masks=masks),), {}

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

		return (request,), {}

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
		return (interface.DeleteRequest(id, [Field(self._engine, path) for path in paths]
			if paths is not None else None),), {}

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

		return (request,), {}

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

		return (interface.CreateRequest(fields),), {}

	def _prepare_objectExists(self, id):
		"""
		Check whether object exists.
		id - object ID
		Returns True or False.
		"""
		return (interface.ObjectExistsRequest(id),), {}

	def _prepare_dump(self):
		"""
		Dump the whole database contents.
		Returns list [obj_id1, contents1, obj_id2, contents2, ...]
		"""
		return (interface.DumpRequest(),), {}

	def _prepare_repair(self):
		"""Rebuild caching tables in database using existing contents."""
		return (interface.RepairRequest(),), {}


class ObjectCache:
	"""
	Class, which partially mimics Connection interface, storing
	object data in Python structures.
	"""

	def __init__(self, remove_conflicts=False, size_threshold=0):
		self._root = {}
		self._access_logger = AccessLogger(size_threshold)
		self._remove_conflicts = remove_conflicts
		self._size_threshold = size_threshold
		self._clearUndoHistory()

	def getRemoveConflicts(self):
		return self._remove_conflicts

	def rollback(self):
		"""Roll back all memorized changes"""

		# in case of LimitedSizeDict usage, some newly created objects
		# can be already deleted from cache
		to_delete = self._created_objects.intersection(self._root)

		for id in to_delete:
			del self._root[id]
		for id in self._modified_objects:
			self._root[id] = self._modified_objects[id]

		self._clearUndoHistory()

	def commit(self):
		"""Confirm all changes made from previous commit/rollback"""

		# if we're not logging access, just clear history and return
		if self._size_threshold == 0:
			self._clearUndoHistory()
			return

		# update access for created objects
		# (additional check is because they could be deleted
		# during the same transaction)
		for id in self._created_objects:
			if id in self._root:
				self._access_logger.update(id)

		# update access for modified objects
		for id in self._modified_objects:
			if id in self._root:
				self._access_logger.update(id)
			else:
				self._access_logger.delete(id)

		# remove least recently accessed objects from cache
		oldest = self._access_logger.delete_oldest()
		for id in oldest:
			del self._root[id]

		self._clearUndoHistory()

	def _clearUndoHistory(self):
		"""Forget all memorized changes"""
		self._created_objects = set()
		self._modified_objects = {}

	def _deleteAll(self, obj, path):
		"""Delete all values from given path"""
		if path[0] is None and isinstance(obj, list):
			if len(path) == 1:
				obj[:] = []
			else:
				for i in range(len(obj)):
					self._deleteAll(obj[i], path[1:])
		elif (isinstance(path[0], str) and isinstance(obj, dict) and path[0] in obj.keys()) or \
				(isinstance(path[0], int) and isinstance(obj, list) and path[0] < len(obj)):
			if len(path) == 1:
				del obj[path[0]]
			else:
				self._deleteAll(obj[path[0]], path[1:])

	def _memorize_created(self, id):
		if id not in self._created_objects and id not in self._modified_objects:
			self._created_objects.add(id)

	def _memorize_modified(self, id):
		if id not in self._created_objects and id not in self._modified_objects:
			self._modified_objects[id] = self._root[id]

	def create(self, id, data, path=None):
		self._memorize_created(id)

		if path is None:
			path = []

		saveToTree(self._root, id, path, copy.deepcopy(data))

	def modify(self, id, path, value, remove_conflicts=None):
		self._memorize_modified(id)

		if path is None:
			path = []
		if remove_conflicts is None:
			remove_conflicts = self._remove_conflicts
		saveToTree(self._root, id, path, copy.deepcopy(value),
			remove_conflicts=remove_conflicts)

	def insert(self, id, path, value, remove_conflicts=None):
		self.insertMany(id, path, [value], remove_conflicts=remove_conflicts)

	def insertMany(self, id, path, values, remove_conflicts=None):
		self._memorize_modified(id)

		if remove_conflicts is None:
			remove_conflicts = self._remove_conflicts

		values = [copy.deepcopy(value) for value in values]

		if remove_conflicts:
			self.modify(id, path[:-1], [], remove_conflicts=True)

		try:
			target = getNodeByPath(self._root[id], path[:-1])
		except:
			saveToTree(self._root, id, path[:-1], [])
			target = getNodeByPath(self._root[id], path[:-1])
		index = path[-1]

		if index is not None and index > len(target):
			for i in range(len(target), index):
				target.append(None)

		if index is None:
			for value in values:
				target.append(value)
		else:
			for value in reversed(values):
				target.insert(index, value)

	def delete(self, id, path=None):
		self.deleteMany(id, paths=[path] if path is not None else None)

	def deleteMany(self, id, paths=None):
		self._memorize_modified(id)

		if paths is None:
			del self._root[id]
		else:
			for path in paths:
				self._deleteAll(self._root[id], path)

	def readByMask(self, id, mask=None):
		return self.read(id, path=None, masks=[mask] if mask is not None else None)

	def readByMasks(self, id, masks=None):
		return self.read(id, path=None, masks=masks)

	def read(self, id, path=None, masks=None):
		if path is None:
			path = []

		try:
			res = getNodeByPath(self._root, [id] + path)
		except:
			raise interface.LogicError("Object " + str(id) + " does not have field " +
				str(path))

		if masks is not None:
			fields = treeToPaths(res)
			res_fields = []
			for field_path, value in fields:
				for mask in masks:
					if pathMatchesMask(field_path, mask):
						res_fields.append((field_path, value))
						break

			if len(res_fields) == 0:
				raise interface.LogicError("Object " + str(id) +
					" does not have fields matching given masks")

			res = pathsToTree(res_fields)

		return copy.deepcopy(res)

	def objectExists(self, id):
		return id in self._root

	def invalidate(self):
		for id in self._root:
			self._memorize_modified(id)

		self._root = {}

	def getIDs(self):
		return set(self._root)


class CachedConnection(TransactedConnection):
	"""
	Wrapper on top of object with Connection interface, which provides
	object data caching.
	Warning: do not use if two or more simultaneous connections to one DB are
	opened - cache can return old data in this case.
	"""

	def __init__(self, conn, size_threshold=0):
		TransactedConnection.__init__(self)
		self._conn = conn

		self._cache = ObjectCache(remove_conflicts=self._conn.getRemoveConflicts(),
			size_threshold=size_threshold)

		self._sync_handlers = {
			'create': self._handleSyncCreation,
			'modify': self._handleSyncModification,
			'insert': self._handleSyncModification,
			'insertMany': self._handleSyncModification,
			'delete': self._handleSyncModification,
			'deleteMany': self._handleSyncModification,
			'objectExists': self._handleSyncObjectExists,
			'repair': self._handleSyncRepair,
			'read': self._handleSyncRead,
			'readByMask': self._handleSyncRead,
			'readByMasks': self._handleSyncRead
		}

	def getRemoveConflicts(self):
		return self._cache.getRemoveConflicts()

	def _begin(self, sync):
		if sync:
			self._conn.beginSync()

	def _commit(self):
		self._cache.commit()
		self._conn.commit()

	def _rollback(self):
		self._cache.rollback()
		self._conn.rollback()

	def close(self):
		self._conn.close()

	def _onError(self):
		self._cache.rollback()
		TransactedConnection._onError(self)

	def _handleSyncCreation(self, name, value, path=None):
		# Get new ID from connection and create object in cache
		new_id = self._conn.create(value, path)
		self._cache.create(new_id, value, path)
		return new_id

	def _handleSyncModification(self, name, *args, **kwds):
		# Object modification - perform on connection,
		# update cache if necessary and perform same
		# operation on cache.
		getattr(self._conn, name)(*args, **kwds)
		id = args[0]
		if not self._cache.objectExists(id):
			self._cache.create(id, self._conn.read(id))
		getattr(self._cache, name)(*args, **kwds)

	def _handleSyncObjectExists(self, name, id):
		# If object is already in cache, the answer is trivial;
		# if not - ask connection object.
		if not self._cache.objectExists(id):
			return self._conn.objectExists(id)
		else:
			return True

	def _handleSyncRepair(self, name):
		# theoretically, after repair anything can happen,
		# so cache is no longer valid
		self._cache.invalidate()
		self._conn.repair()

	def _handleSyncRead(self, name, id, *args, **kwds):
		# Read to cache first if necessary, then read from cache
		if not self._cache.objectExists(id):
			self._cache.create(id, self._conn.read(id))
		return getattr(self._cache, name)(id, *args, **kwds)

	def _handleSync(self, requests):
		"""Handle requests during synchronous transaction"""

		results = []
		for name, args, kwds in requests:
			if name in self._sync_handlers:
			# if request requires some special treatment
				result = self._sync_handlers[name](name, *args, **kwds)
			else:
			# non-cached request, just ask connection
				result = getattr(self._conn, name)(*args, **kwds)

			results.append(result)
		return results

	def _handleAsync(self, requests):
		"""Handle requests during asynchronous transaction"""

		# memorizing all objects, cached during this transaction
		cached_ids = self._cache.getIDs()

		# for each request, if corresponding element of this array is True,
		# it means that additional request was performed, reading or
		# checking object existence
		additional_requests = []

		# TODO: probably this select-case can be turned into map of handlers

		# First stage: perform asynchronous requests and read result

		for name, args, kwds in requests:
			additional_request = False

			if name == 'commit':
			# read results of asynchronous transaction
				raw_results = self._conn.commit()

			elif name in ['modify', 'insert', 'insertMany', 'delete', 'deleteMany']:
			# modification request - check if object should be cached
				id = args[0]
				if id not in cached_ids:
					additional_request = True
					self._conn.read(id)
					cached_ids.add(id)
				getattr(self._conn, name)(*args, **kwds)

			elif name in ['read', 'readByMask', 'readByMasks']:
			# read request - check if object should be cached
			# do not make initial request, because its result
			# will be taken straight from cache
				id = args[0]
				if id not in cached_ids:
					additional_request = True
					self._conn.read(id)
					cached_ids.add(id)

			elif name == 'objectExists':
			# checking if object exists - if it is not cached,
			# we will have to ask connection
				id = args[0]
				if id not in cached_ids:
					additional_request = True
					getattr(self._conn, name)(*args, **kwds)

			elif name == 'repair':
			# invalidate all cached IDs
				cached_ids.clear()
				getattr(self._conn, name)(*args, **kwds)

			else:
			# just pass request to connection
				getattr(self._conn, name)(*args, **kwds)
			additional_requests.append(additional_request)

		# Second stage - remove results of additional requests from list
		# and update cache

		raw_results = [None] + list(reversed(raw_results)) + [None]
		results = []

		for elem, additional_request in zip(requests, additional_requests):
			name, args, kwds = elem
			result = None

			if name == 'commit':
				self._cache.commit()

			elif name == 'create':
			# take new ID from request result and create new object in cache
				new_id = raw_results.pop()
				self._cache.create(new_id, *args, **kwds)
				result = new_id

			elif name in ['modify', 'insert', 'insertMany', 'delete', 'deleteMany']:
			# update cache if there was additional request
				id = args[0]
				if additional_request:
					self._cache.create(id, raw_results.pop())
				getattr(self._cache, name)(*args, **kwds)
				raw_results.pop()

			elif name == 'objectExists':
			# if there was additional request, take result from it
			# if not, take result from cache
				id = args[0]
				if additional_request:
					result = raw_results.pop()
				else:
					result = True

			elif name in ['read', 'readByMask', 'readByMasks']:
			# if there was additional request, update cache
			# then result of initial request is definitely in cache
				id = args[0]
				if additional_request:
					self._cache.create(id, raw_results.pop())
				result = getattr(self._cache, name)(*args, **kwds)

			elif name == 'repair':
				self._cache.invalidate()
			else:
				result = raw_results.pop()

			results.append(result)

		# cut begin() and commit() results
		return results[1:-1]

	def _handleRequests(self, requests):
		if self._sync():
			return self._handleSync(requests)
		else:
			return self._handleAsync(requests)
