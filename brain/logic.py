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
		self._ID_TABLE = self._engine.getSafeName(
			self._engine.getNameString(["id"]))

		self._LISTSIZES_TABLE = self._engine.getSafeName(
			self._engine.getNameString(["listsizes"]))

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
		self._engine.execute(("CREATE table IF NOT EXISTS {id_table} " +
			"({id_column} {id_type}, {field_column} {text_type}, " +
			"{type_column} {text_type}, " +
			"{refcount_column} {refcount_type})").format(
			field_column=self._FIELD_COLUMN,
			id_column=self._ID_COLUMN,
			id_table=self._ID_TABLE,
			id_type=self._ID_TYPE,
			refcount_column=self._REFCOUNT_COLUMN,
			refcount_type=self._INT_TYPE,
			text_type=self._TEXT_TYPE,
			type_column=self._TYPE_COLUMN))

		# create support table which holds maximum list index for each list
		# existing in database
		self._engine.execute(("CREATE table IF NOT EXISTS {listsizes_table} " +
			"({id_column} {id_type}, {field_column} {text_type}, " +
			"{max_column} {list_index_type})").format(
			field_column=self._FIELD_COLUMN,
			id_column=self._ID_COLUMN,
			id_type=self._ID_TYPE,
			listsizes_table=self._LISTSIZES_TABLE,
			list_index_type=self._INT_TYPE,
			max_column=self._MAX_COLUMN,
			text_type=self._TEXT_TYPE))

	def deleteSpecification(self, id):
		"""Delete all information about object from specification table"""
		self._engine.execute("DELETE FROM {id_table} WHERE {id_column}={id}".format(
			id=id,
			id_table=self._ID_TABLE,
			id_column=self._ID_COLUMN))

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
			self._engine.execute("INSERT INTO {id_table} VALUES ({id}, {field_name}, {type}, 1)".format(
				field_name=field.name_as_value_no_type,
				id=id,
				id_table=self._ID_TABLE,
				type=field.type_str_as_value))
		else:
		# otherwise increase the existing reference counter
			self._engine.execute(("UPDATE {id_table} SET {refcount_column}={refcount_column}+1 " +
				"WHERE {id_column}={id} AND {field_column}={field_name} " +
				"AND {type_column}={type}").format(
				field_column=self._FIELD_COLUMN,
				field_name=field.name_as_value_no_type,
				id=id,
				id_column=self._ID_COLUMN,
				id_table=self._ID_TABLE,
				refcount_column=self._REFCOUNT_COLUMN,
				type=field.type_str_as_value,
				type_column=self._TYPE_COLUMN))

	def decreaseRefcount(self, id, field, num=1):
		"""
		Decrease reference count for given field and type
		one can specify a decrement if deleting values by mask

		field should have definite type
		"""

		# build condition for selecting necessary type
		# if type is Null, we should use ISNULL, because '=NULL' won't work
		if field.isNull():
			type_cond = self._TYPE_COLUMN + ' ISNULL'
		else:
			type_cond = self._TYPE_COLUMN + '=' + field.type_str_as_value

		# get current value of reference counter
		l = self._engine.execute(("SELECT {refcount_column} FROM {id_table} " +
			"WHERE {id_column}={id} AND {field_column}={field_name} AND {type_cond}").format(
			field_column=self._FIELD_COLUMN,
			field_name=field.name_as_value_no_type,
			id=id,
			id_column=self._ID_COLUMN,
			id_table=self._ID_TABLE,
			refcount_column=self._REFCOUNT_COLUMN,
			type_cond=type_cond))

		if l[0][0] < num:
		# if for some reason counter value is lower than expected, we will raise
		# exception, because this bug can be hard to catch later
			raise interface.StructureError("Unexpected value of reference counter: " + str(l[0][0]))
		if l[0][0] == num:
		# if these references are the last ones, delete this counter
			self._engine.execute(("DELETE FROM {id_table} " +
				"WHERE {id_column}={id} AND {field_column}={field_name} AND {type_cond}").format(
				field_column=self._FIELD_COLUMN,
				field_name=field.name_as_value_no_type,
				id=id,
				id_column=self._ID_COLUMN,
				id_table=self._ID_TABLE,
				type_cond=type_cond))
		else:
		# otherwise just decrease the counter by given value
			self._engine.execute(("UPDATE {id_table} SET {refcount_column}={refcount_column}-{val} " +
				"WHERE {id_column}={id} AND {field_column}={field_name} " +
				"AND {type_column}={type}").format(
				field_column=self._FIELD_COLUMN,
				field_name=field.name_as_value_no_type,
				id=id,
				id_column=self._ID_COLUMN,
				id_table=self._ID_TABLE,
				refcount_column=self._REFCOUNT_COLUMN,
				type=field.type_str_as_value,
				type_column=self._TYPE_COLUMN, val=num))

	def getValueTypes(self, id, field):
		"""Returns list of value types already stored in given field"""

		# just query specification table for all types for given object and field
		l = self._engine.execute(("SELECT {type_column} FROM {id_table} " +
			"WHERE {id_column}={id} AND {field_column}={field_name}").format(
			field_column=self._FIELD_COLUMN,
			field_name=field.name_as_value_no_type,
			id=id,
			id_column=self._ID_COLUMN,
			id_table=self._ID_TABLE,
			type_column=self._TYPE_COLUMN))

		return [x[0] for x in l]

	def getFieldsList(self, id, field=None, exclude_self=False):
		"""
		Get list of fields of all possible types for given object.
		If field is given, return only those whose names start from its name
		If exclude_self is true, exclude 'field' itself from results
		"""

		if field is not None:
		# If field is given, return only fields, which contain its name in the beginning
			regexp_cond = " AND {field_column} REGEXP {regexp}"
			regexp_val = self._engine.getSafeValue("^" + field.name_str_no_type +
				("." if exclude_self else ""))
			type = field.type_str_as_value
		else:
			regexp_cond = ""
			regexp_val = None
			type = None

		# Get list of fields
		l = self._engine.execute(("SELECT DISTINCT {field_column} FROM {id_table} " +
			"WHERE {id_column}={id}" + regexp_cond).format(
			field_column=self._FIELD_COLUMN,
			id=id,
			id_column=self._ID_COLUMN,
			id_table=self._ID_TABLE,
			regexp=regexp_val,
			type=type,
			type_column=self._TYPE_COLUMN))

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
		l = self._engine.execute("SELECT COUNT() FROM {id_table} WHERE {id_column}={id}".format(
			field_column=self._FIELD_COLUMN,
			id=id,
			id_column=self._ID_COLUMN,
			id_table=self._ID_TABLE))

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
		l = self._engine.execute(("SELECT {value_column}{columns_query} FROM {field_name} " +
			"WHERE {id_column}={id}{columns_condition}").format(
			columns_condition=field.columns_condition,
			columns_query=field.columns_query,
			field_name=field.name_as_table,
			id=id,
			id_column=self._ID_COLUMN,
			value_column=self._VALUE_COLUMN if not field.isNull() else ""))

		# Convert results to list of Fields
		res = []
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

			self._engine.execute(("UPDATE {listsizes_table} " +
				"SET {max_column}={val} " +
				"WHERE {id_column}={id} AND {field_column}={field_name}").format(
				field_column=self._FIELD_COLUMN,
				field_name=field.name_hashstr,
				id=id,
				id_column=self._ID_COLUMN,
				listsizes_table=self._LISTSIZES_TABLE,
				max_column=self._MAX_COLUMN,
				val=val))
		else:
		# create new record
			self._engine.execute(("INSERT INTO {listsizes_table} " +
				"VALUES ({id}, {field_name}, {val})").format(
				field_name=field.name_hashstr,
				id=id,
				listsizes_table=self._LISTSIZES_TABLE,
				val=val))

	def assureFieldTableExists(self, field):
		"""
		Create table for storing values of this field if it does not exist yet

		field should have definite type
		"""

		# Create table
		self._engine.execute("CREATE TABLE IF NOT EXISTS {field_name} ({values_str})"
			.format(field_name=field.name_as_table,
			values_str=field.getCreationStr(self._ID_COLUMN,
				self._VALUE_COLUMN, self._ID_TYPE, self._INT_TYPE)))

	def getMaxListIndex(self, id, field):
		"""Get maximum value of list index for the undefined column of the field"""

		l = self._engine.execute(("SELECT {max_column} FROM {listsizes_table} " +
			"WHERE {id_column}={id} AND {field_column}={field_name}").format(
			field_column=self._FIELD_COLUMN,
			field_name=field.name_hashstr,
			id=id,
			id_column=self._ID_COLUMN,
			listsizes_table=self._LISTSIZES_TABLE,
			max_column=self._MAX_COLUMN))

		if len(l) > 0:
			return l[0][0]
		else:
			return None

	def buildSqlQuery(self, condition):
		"""Recursive function to transform condition into SQL query"""

		if condition is None:
			return ("SELECT DISTINCT {id_column} FROM {id_table}").format(
				id_column=self._ID_COLUMN,
				id_table=self._ID_TABLE)

		if not condition.leaf:
			# child conditions
			cond1 = self.buildSqlQuery(condition.operand1)
			cond2 = self.buildSqlQuery(condition.operand2)

			# mapping to SQL operations
			operations = {
				op.AND: 'INTERSECT',
				op.OR: 'UNION'
			}

			return ("SELECT * FROM ({cond1}) {operation} SELECT * FROM ({cond2})"
				.format(cond1=cond1, cond2=cond2,
				operation=operations[condition.operator]))

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
			return self._engine.getEmptyCondition()

		safe_name = condition.operand1.name_as_table
		not_str = " NOT " if condition.invert else " "

		# mapping to SQL comparisons
		comparisons = {
			op.EQ: '=',
			op.REGEXP: 'REGEXP',
			op.LT: '<',
			op.GT: '>',
			op.LTE: '<=',
			op.GTE: '>='
		}

		# build query

		if op2 is not None or not condition.invert:

			# construct comparing condition
			if op2 is not None:
				comp_str = "WHERE{not_str} {value_column} {comp} {val}".format(
					comp=comparisons[condition.operator],
					not_str=not_str,
					val=self._engine.getSafeValue(op2),
					value_column=self._VALUE_COLUMN)
			else:
				comp_str = ""

			# construct query
			result = ("SELECT DISTINCT {id_column} FROM {field_name} " +
				"{comp_str}{columns_condition}").format(
				columns_condition=op1.columns_condition,
				comp_str=comp_str,
				field_name=safe_name,
				id_column=self._ID_COLUMN)

			if condition.invert:
			# we will add objects that do not even have such field
				result += " UNION "
		else:
			result = ""

		# if we need to invert results, we have to add all objects that do
		# not have this field explicitly, because they won't be caught by previous query
		if condition.invert:
			result += ("SELECT {id_column} FROM (SELECT DISTINCT {id_column} FROM {id_table} " +
				"EXCEPT SELECT DISTINCT {id_column} FROM {field_name})").format(
				field_name=safe_name,
				id_column=self._ID_COLUMN,
				id_table=self._ID_TABLE)

		return result

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
			self._engine.execute(("UPDATE {field_name} " +
				"SET {col_name}={col_name}+{shift} " +
				"WHERE {id_column}={id}{cond} AND {col_name}>={col_val}").format(
				col_name=col_name,
				col_val=col_val,
				cond=cond,
				field_name=field.name_as_table,
				id=id,
				id_column=self._ID_COLUMN,
				shift=shift))

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
			query_str = "FROM {field_name} WHERE {id_column}={id}{delete_condition}".format(
				delete_condition=condition,
				field_name=field_copy.name_as_table,
				id=id,
				id_column=self._ID_COLUMN)

			# get number of records to be deleted
			res = self._engine.execute("SELECT COUNT() " + query_str)
			del_num = res[0][0]

			# if there are any, delete them
			if del_num > 0:
				self.decreaseRefcount(id, field_copy, num=del_num)
				self._engine.execute("DELETE " + query_str)

				# check if the table is empty and if it is - delete it too
				if self._engine.tableIsEmpty(field_copy.name_str):
					self._engine.deleteTable(field_copy.name_str)

	def addValueRecord(self, id, field):
		"""
		Add value to the corresponding table

		field should have definite type
		"""

		new_value = (", " + field.safe_value) if not field.isNull() else ""
		self._engine.execute(("INSERT INTO {field_name} " +
			"VALUES ({id}{value}{columns_values})").format(
			columns_values=field.columns_values,
			field_name=field.name_as_table,
			id=id,
			value=new_value))


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

		request = self._structure.buildSqlQuery(request.condition)
		result = self._engine.execute(request)
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
