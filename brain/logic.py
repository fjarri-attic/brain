"""Module with classes, describing DDB wrapper over SQL"""

import sqlite3
import re
import copy
import functools

from . import interface
from .interface import Field
from . import op

class _StructureLayer:
	"""Class which is connected to DB engine and incapsulates all SQL queries"""

	_ID_COLUMN = 'id' # name of column with object id in all tables

	# column names for specification table
	_FIELD_COLUMN = 'field' # field names
	_TYPE_COLUMN = 'type' # field types
	_REFCOUNT_COLUMN = 'refcount' # number of records with this type

	_MAX_COLUMN = 'max' # name of column with maximum list index values
	_VALUE_COLUMN = 'value' # name of column with field values


	def __init__(self, engine):
		self._engine = engine

		# memorize strings with support table names
		self._ID_TABLE = self._engine.getNameString(["id"])
		self._LISTSIZES_TABLE = self._engine.getNameString(["listsizes"])

		# types for support tables
		self._ID_TYPE = self._engine.getIdType()
		self._TEXT_TYPE = self._engine.getColumnType(str())
		self._INT_TYPE = self._engine.getColumnType(int())

		# create support tables
		self._engine.begin()
		self._createSupportTables()
		self._engine.commit()


	def _createSupportTables(self):
		"""Create database support tables (sort of caching)"""

		# create specification table, which holds field names, their types
		# and number of records of each type for all database objects
		if not self._engine.tableExists(self._ID_TABLE):
			self._engine.execute("CREATE table {} " +
				"({id_column} {id_type}, {field_column} {text_type}, " +
				"{type_column} {text_type}, " +
				"{refcount_column} {refcount_type})",
				[self._ID_TABLE],
				{'field_column': self._FIELD_COLUMN,
				'id_column': self._ID_COLUMN,
				'id_type': self._ID_TYPE,
				'refcount_column': self._REFCOUNT_COLUMN,
				'refcount_type': self._INT_TYPE,
				'text_type': self._TEXT_TYPE,
				'type_column': self._TYPE_COLUMN})

		# create support table which holds maximum list index for each list
		# existing in database
		if not self._engine.tableExists(self._LISTSIZES_TABLE):
			self._engine.execute("CREATE table {} " +
				"({id_column} {id_type}, {field_column} {text_type}, " +
				"{max_column} {list_index_type})",
				[self._LISTSIZES_TABLE],
				{'field_column' :self._FIELD_COLUMN,
				'id_column': self._ID_COLUMN,
				'id_type': self._ID_TYPE,
				'list_index_type': self._INT_TYPE,
				'max_column': self._MAX_COLUMN,
				'text_type': self._TEXT_TYPE})

	def deleteSpecification(self, id):
		"""Delete all information about object from specification table"""
		self._engine.execute("DELETE FROM {} WHERE {id_column}=?",
			[self._ID_TABLE], {'id_column': self._ID_COLUMN}, [id])

	def increaseRefcount(self, id, field, new_type):
		"""
		Increase reference counter of given field and type (or create it)
		new_field=True means that field with this name does not exist in this object
		new_type=True means that field with this name and this type does not exist
		in this object

		field should have definite type
		"""

		if new_type:
		# if adding a value of new type to existing field,
		# add a reference counter for this field and this type
			self._engine.execute("INSERT INTO {} VALUES (?, ?, ?, 1)",
				[self._ID_TABLE], None, [id, field.name_str_no_type,
				field.type_str])
		else:
		# otherwise increase the existing reference counter
			self._engine.execute("UPDATE {} " +
				"SET {refcount_column}={refcount_column}+1 " +
				"WHERE {id_column}=? AND {field_column}=? " +
				"AND {type_column}=?",
				[self._ID_TABLE],
				{'field_column': self._FIELD_COLUMN,
				'id_column': self._ID_COLUMN,
				'refcount_column': self._REFCOUNT_COLUMN,
				'type_column': self._TYPE_COLUMN},
				[id, field.name_str_no_type, field.type_str])

	def decreaseRefcount(self, id, field, num=1):
		"""
		Decrease reference count for given field and type
		one can specify a decrement if deleting values by mask

		field should have definite type
		"""
		isnull = field.isNull()
		type_cond = ' ISNULL' if isnull else '=?'
		type_cond_val = [] if isnull else [field.type_str]

		# get current value of reference counter
		l = self._engine.execute("SELECT {refcount_column} FROM {} " +
			"WHERE {id_column}=? AND {field_column}=? " +
			"AND {type_column}" + type_cond,
			[self._ID_TABLE],
			{'field_column': self._FIELD_COLUMN,
			'id_column': self._ID_COLUMN,
			'refcount_column': self._REFCOUNT_COLUMN,
			'type_column': self._TYPE_COLUMN},
			[id, field.name_str_no_type] + type_cond_val)

		if l[0][0] < num:
		# if for some reason counter value is lower than expected, we will raise
		# exception, because this bug can be hard to catch later
			raise interface.StructureError("Unexpected value of reference counter: " + str(l[0][0]))
		if l[0][0] == num:
		# if these references are the last ones, delete this counter
			self._engine.execute("DELETE FROM {} " +
				"WHERE {id_column}=? AND {field_column}=? " +
				"AND {type_column}" + type_cond,
				[self._ID_TABLE],
				{'field_column': self._FIELD_COLUMN,
				'id_column': self._ID_COLUMN,
				'type_column': self._TYPE_COLUMN},
				[id, field.name_str_no_type] + type_cond_val)
		else:
		# otherwise just decrease the counter by given value
			self._engine.execute("UPDATE {} SET {refcount_column}={refcount_column}-? " +
				"WHERE {id_column}=? AND {field_column}=? " +
				"AND {type_column}=?",
				[self._ID_TABLE],
				{'field_column': self._FIELD_COLUMN,
				'id_column': self._ID_COLUMN,
				'refcount_column': self._REFCOUNT_COLUMN,
				'type_column': self._TYPE_COLUMN},
				[num, id, field.name_str_no_type, field.type_str])

	def getValueTypes(self, id, field):
		"""Returns list of value types already stored in given field"""

		# just query specification table for all types for given object and field
		l = self._engine.execute("SELECT {type_column} FROM {} " +
			"WHERE {id_column}=? AND {field_column}=?",
			[self._ID_TABLE],
			{'field_column': self._FIELD_COLUMN,
			'id_column': self._ID_COLUMN,
			'type_column': self._TYPE_COLUMN},
			[id, field.name_str_no_type])

		return [x[0] for x in l]

	def getFieldsList(self, id, field=None, exclude_self=False):
		"""
		Get list of fields of all possible types for given object.
		If field is given, return only those whose names start from its name
		If exclude_self is true, exclude 'field' itself from results
		"""

		if field is not None:
		# If field is given, return only fields, which contain its name in the beginning
			regexp_cond = " AND {field_column} {regexp_op} ?"
			regexp_val = ["^" + field.name_str_no_type +
				("." if exclude_self else "")]
			regexp_op = self._engine.getRegexpOp()
		else:
			regexp_cond = ""
			regexp_val = []
			regexp_op = None

		# Get list of fields
		l = self._engine.execute("SELECT DISTINCT {field_column} FROM {} " +
			"WHERE {id_column}=?" + regexp_cond,
			[self._ID_TABLE],
			{'field_column': self._FIELD_COLUMN,
			'id_column': self._ID_COLUMN,
			'regexp_op': regexp_op},
			[id] + regexp_val)

		# fill the beginnings of found field names with the name of
		# given field (if any) or just construct result list
		res = []
		for elem in l:
			fld = Field.fromNameStr(self._engine, elem[0])
			if field is not None:
				fld.name[:len(field.name)] = field.name
			res.append(fld)

		return res

	def objectExists(self, id):
		"""Check if object exists in database"""

		# We need just check if there is at least one row with its id
		# in specification table
		l = self._engine.execute("SELECT COUNT(*) FROM {} WHERE {id_column}=?",
			[self._ID_TABLE], {'id_column': self._ID_COLUMN}, [id])

		return l[0][0] > 0


	def getFieldValue(self, id, field):
		"""
		Read value of given field(s)

		field should have definite type
		"""

		# if there is no such field - nothing to do
		if not self._engine.tableExists(field.name_str):
			return None

		# Get field values
		# If field is a mask (i.e., contains Nones), there will be more than one result
		l = self._engine.execute("SELECT {value_column}{columns_query} FROM {} " +
			"WHERE {id_column}=?{columns_condition}",
			[field.name_str],
			{'columns_condition': field.columns_condition,
			'columns_query': field.columns_query,
			'id_column': self._ID_COLUMN,
			'value_column': self._VALUE_COLUMN if not field.isNull() else ""},
			[id])

		# Convert results to list of Fields
		res = []
		l = [tuple(x) for x in l]
		for elem in l:
			if field.isNull():
			# in NULL table there is no values, all columns are list indexes
				list_indexes = elem
				value = None
			else:
			# in non-NULL table first element is a value itself
				list_indexes = elem[1:]
				value = elem[0]

			res.append(Field(self._engine,
				field.getDeterminedName(list_indexes), value))

		return res

	def updateListSize(self, id, field):
		"""Update information about the size of given list"""

		# get current maximum list index for given list
		max = self.getMaxListIndex(id, field)
		val = field.name[-1]

		if max is not None:
		# if there is a list, and given value is greater than maximum index, update it

			if max > val: return

			self._engine.execute("UPDATE {} SET {max_column}=? " +
				"WHERE {id_column}=? AND {field_column}=?",
				[self._LISTSIZES_TABLE],
				{'field_column': self._FIELD_COLUMN,
				'id_column': self._ID_COLUMN,
				'max_column': self._MAX_COLUMN},
				[val, id, field.name_hashstr])
		else:
		# create new record
			self._engine.execute("INSERT INTO {} VALUES (?, ?, ?)",
				[self._LISTSIZES_TABLE], None,
				[id, field.name_hashstr, val])

	def assureFieldTableExists(self, field):
		"""
		Create table for storing values of this field if it does not exist yet

		field should have definite type
		"""

		if not self._engine.tableExists(field.name_str):
			self._engine.execute("CREATE TABLE {} ({values_str})",
				[field.name_str],
				{'values_str': field.getCreationStr(self._ID_COLUMN,
				self._VALUE_COLUMN, self._ID_TYPE, self._INT_TYPE)})

	def getMaxListIndex(self, id, field):
		"""Get maximum value of list index for the undefined column of the field"""

		l = self._engine.execute("SELECT {max_column} FROM {} " +
			"WHERE {id_column}=? AND {field_column}=?",
			[self._LISTSIZES_TABLE],
			{'field_column': self._FIELD_COLUMN,
			'id_column': self._ID_COLUMN,
			'max_column': self._MAX_COLUMN},
			[id, field.name_hashstr])

		if len(l) > 0:
			return l[0][0]
		else:
			return None

	def buildSqlQuery(self, condition):
		"""Recursive function to transform condition into SQL query"""

		if condition is None:
			return "SELECT DISTINCT " + self._ID_COLUMN + " FROM {}", [self._ID_TABLE], []

		if not condition.leaf:
			# child conditions
			cond1, tables1, values1 = self.buildSqlQuery(condition.operand1)
			cond2, tables2, values2 = self.buildSqlQuery(condition.operand2)

			# mapping to SQL operations
			operations = {
				op.AND: 'INTERSECT',
				op.OR: 'UNION'
			}

			if cond1 == None and cond2 == None:
				return None, None, None

			if cond1 == None:
				if condition.operator == op.AND:
					return None, None, None
				if condition.operator == op.OR:
					return cond2, tables2, values2

			if cond2 == None:
				if condition.operator == op.AND:
					return None, None, None
				if condition.operator == op.OR:
					return cond1, tables1, values1

			return "SELECT * FROM (" + cond1 + ") as temp " + \
				operations[condition.operator] + " SELECT * FROM (" + \
				cond2 + ") as temp", tables1 + tables2, values1 + values2

		# Leaf condition
		op1 = condition.operand1 # it must be Field
		op2 = condition.operand2 # it must be some value

		# set proper type for the field
		if op2 is not None:
			op1.type_str = self._engine.getColumnType(op2)
		else:
			op1.type_str = None

		# If table with given field does not exist, just return empty query
		if not self._engine.tableExists(op1.name_str):
			return None, None, None

		not_str = " NOT " if condition.invert else " "

		# mapping to SQL comparisons
		comparisons = {
			op.EQ: '=',
			op.REGEXP: self._engine.getRegexpOp(),
			op.LT: '<',
			op.GT: '>',
			op.LTE: '<=',
			op.GTE: '>='
		}

		# build query
		tables = []
		values = []

		if op2 is not None or not condition.invert:

			# construct comparing condition
			if op2 is not None:
				comp_str = "WHERE " + not_str + " " + self._VALUE_COLUMN + " " + \
					comparisons[condition.operator] + " ?"
				values = [op2]
			else:
				comp_str = ""

			# construct query
			result = "SELECT DISTINCT " + self._ID_COLUMN + " FROM {} " + \
				comp_str + " " + op1.columns_condition
			tables = [condition.operand1.name_str]

			if condition.invert:
			# we will add objects that do not even have such field
				result += " UNION "
		else:
			result = ""

		# if we need to invert results, we have to add all objects that do
		# not have this field explicitly, because they won't be caught by previous query
		if condition.invert:
			result += "SELECT " + self._ID_COLUMN + " FROM " + \
				"(SELECT DISTINCT " + self._ID_COLUMN + " FROM {} " + \
				"EXCEPT SELECT DISTINCT " + self._ID_COLUMN + " FROM {}) as temp"
			tables += [self._ID_TABLE, condition.operand1.name_str]

		return result, tables, values

	def renumberList(self, id, target_field, field, shift):
		"""
		Shift indexes in given list
		target_field - points to list which is being processed
		field - child field for one of the elements of this list
		"""

		# Get the name and the value of last numerical column
		col_name, col_val = target_field.getLastListColumn()
		cond = target_field.renumber_condition

		# renumber list indexes for all types
		types = self.getValueTypes(id, field)
		for type in types:
			field.type_str = type
			self._engine.execute("UPDATE {} " +
				"SET " + col_name + "=" + col_name + "+? " +
				"WHERE " + self._ID_COLUMN + "=?" + cond +
				" AND " + col_name + ">=?",
				[field.name_str], None, [shift, id, col_val])

	def deleteValues(self, id, field, condition=None):
		"""Delete value of given field(s)"""

		field_copy = Field(self._engine, field.name)

		# if there is no special condition, take the mask of given field
		if condition is None:
			condition = field.columns_condition

		types = self.getValueTypes(id, field_copy)
		for type in types:

			# prepare query for deletion
			field_copy.type_str = type
			query_str = "FROM {} WHERE " + self._ID_COLUMN + "=?" + condition
			tables = [field_copy.name_str]
			values = [id]

			# get number of records to be deleted
			res = self._engine.execute("SELECT COUNT(*) " + query_str, tables, None, values)
			del_num = res[0][0]

			# if there are any, delete them
			if del_num > 0:
				self.decreaseRefcount(id, field_copy, num=del_num)
				self._engine.execute("DELETE " + query_str, tables, None, values)

				# check if the table is empty and if it is - delete it too
				if self._engine.tableIsEmpty(field_copy.name_str):
					self._engine.deleteTable(field_copy.name_str)

	def addValueRecord(self, id, field):
		"""
		Add value to the corresponding table

		field should have definite type
		"""

		new_value = ", ?" if not field.isNull() else ""
		self._engine.execute("INSERT INTO {} " +
			"VALUES (?" + new_value + field.columns_values + ")",
			[field.name_str], None, [id] + ([] if field.isNull() else [field.value]))


class LogicLayer:
	"""Class, representing DDB logic"""

	def __init__(self, engine):
		self._engine = engine
		self._structure = _StructureLayer(engine)

	def _deleteField(self, id, field):
		"""Delete given field(s)"""

		if field.pointsToListElement():
			# deletion of list element requires renumbering of other elements
			self._renumber(id, field, -1)
		else:
			# otherwise just delete values using given field mask
			self._structure.deleteValues(id, field)

	def _removeConflicts(self, id, field):
		"""
		Check that adding this field does not break the database structure, namely:
		given field can either contain value, or list, or map, not several at once
		"""

		# check all ancestor fields in hierarchy
		for anc, last in field.ancestors(include_self=False):

			# delete all values whose names are a part of the name of field to add
			# in other words, no named maps or lists
			types = self._structure.getValueTypes(id, anc)
			for type in types:
				anc.type_str = type
				self._deleteField(id, anc)

			self._checkForListAndMapConflicts(id, anc, last)

		# check separately for root level lists and maps
		self._checkForListAndMapConflicts(id, None, field.name[0])

	def _checkForListAndMapConflicts(self, id, field, last):
		"""
		Check that adding this field will not mean adding list elements to map
		or map keys to list. If field is None, root level is checked.
		"""

		# Get all fields with names, starting from name_copy, excluding
		# the one whose name equals name_copy
		relatives = self._structure.getFieldsList(id, field, exclude_self=True)

		# we have to check only first field in list
		# if there are no conflicts, other fields do not conflict too
		if len(relatives) > 0:
			elem = relatives[0].name[len(field.name) if field is not None else 0]

			if isinstance(last, str) and not isinstance(elem, str):
				raise interface.StructureError("Cannot modify map, when list already exists on this level")
			if not isinstance(last, str) and isinstance(elem, str):
				raise interface.StructureError("Cannot modify list, when map already exists on this level")


	def _addFieldToSpecification(self, id, field):
		"""Check if field conforms to hierarchy and if yes, add it"""

		# check if there are already field with this name in object
		types = self._structure.getValueTypes(id, field)

		# if adding a new field, ensure that there will be
		# no conflicts in database structure
		if len(types) == 0:
			self._removeConflicts(id, field)

		self._structure.increaseRefcount(id, field, new_type=(not field.type_str in types))

	def _setFieldValue(self, id, field):
		"""Set value of given field"""

		# Update maximum values cache
		for anc, last in field.ancestors(include_self=True):
			if anc.pointsToListElement():
				self._structure.updateListSize(id, anc)

		# Delete old value (checking all tables because type could be different)
		self._structure.deleteValues(id, field)

		# Create field table if it does not exist yet
		self._structure.assureFieldTableExists(field)
		self._addFieldToSpecification(id, field) # create object header
		self._structure.addValueRecord(id, field) # Insert new value

	def _deleteObject(self, id):
		"""Delete object with given ID"""

		fields = self._structure.getFieldsList(id)

		# for each field, remove it from tables
		for field in fields:
			self._deleteField(id, field)

		self._structure.deleteSpecification(id)

	def _renumber(self, id, target_field, shift):
		"""Renumber list elements before insertion or deletion"""

		# Get all child field names
		fields_to_reenum = self._structure.getFieldsList(id, target_field)
		for fld in fields_to_reenum:

			# if shift is negative, we should delete elements first
			if shift < 0:
				self._structure.deleteValues(id, fld, target_field.columns_condition)

			# shift numbers of all elements in list
			self._structure.renumberList(id, target_field, fld, shift)

	def _modifyFields(self, id, fields):
		for field in fields:
			self._setFieldValue(id, field)

	def processCreateRequest(self, request):
		new_id = self._engine.getNewId()
		self._modifyFields(new_id, request.fields)
		return new_id

	def processModifyRequest(self, request):
		self._modifyFields(request.id, request.fields)

	def processDeleteRequest(self, request):

		if request.fields is not None:
			# remove specified fields
			for field in request.fields:
				self._deleteField(request.id, field)
			return
		else:
			# delete whole object
			self._deleteObject(request.id)

	def processReadRequest(self, request):

		# check if object exists first
		if not self._structure.objectExists(request.id):
			raise interface.LogicError("Object " + str(request.id) + " does not exist")

		# if list of fields was not given, read all object's fields
		if request.fields is None:
			fields = self._structure.getFieldsList(request.id)
		else:
			res = []
			for field in request.fields:
				res += self._structure.getFieldsList(request.id, field)
			fields = res

		result_list = []
		for field in fields:
			for type in self._structure.getValueTypes(request.id, field):
				field.type_str = type
				res = self._structure.getFieldValue(request.id, field)
				if res is not None:
					result_list += res

		return result_list

	def processSearchRequest(self, request):
		"""Search for all objects using given search condition"""

		request, tables, values = self._structure.buildSqlQuery(request.condition)
		if request is None:
			return []
		result = self._engine.execute(request, tables, None, values)
		list_res = [x[0] for x in result]

		return list_res

	def processInsertRequest(self, request):

		def enumerate(field_groups, col_num, starting_num):
			"""Enumerate given column in list of fields"""
			counter = starting_num
			for field_group in field_groups:
				for field in field_group:
					field.name[col_num] = counter
				counter += 1

		target_col = len(request.path.name) - 1 # last column in name of target field

		max = self._structure.getMaxListIndex(request.id, request.path)
		if max is None:
		# list does not exist yet
			enumerate(request.field_groups, target_col, 0)
		elif request.path.name[target_col] is None:
		# list exists and we are inserting elements to the end
			starting_num = max + 1
			enumerate(request.field_groups, target_col, starting_num)
		else:
		# list exists and we are inserting elements to the beginning or to the middle
			self._renumber(request.id, request.path, len(request.field_groups))
			enumerate(request.field_groups, target_col, request.path.name[target_col])

		fields = functools.reduce(list.__add__, request.field_groups, [])
		self._modifyFields(request.id, fields)

	def processObjectExistsRequest(self, request):
		return self._structure.objectExists(request.id)

	def processDumpRequest(self, request):
		ids = self.processSearchRequest(interface.SearchRequest())
		return {obj_id: self.processReadRequest(interface.ReadRequest(obj_id))
			for obj_id in ids}
