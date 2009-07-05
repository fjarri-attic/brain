"""Module with classes, describing DDB wrapper over SQL"""

import sqlite3
import re
import copy

from . import interface


class _InternalField:
	"""Class for more convenient handling of Field objects"""

	def __init__(self, engine, name, value=None):
		self.__engine = engine
		self.name = name[:]
		self.__value = value

	@classmethod
	def fromNameStr(cls, engine, name_str, value=None):
		"""Create object using stringified name instead of list"""

		# cut prefix 'field' from the resulting list
		return cls(engine, engine.getNameList(name_str)[1:], value)

	@classmethod
	def fromField(cls, engine, field):
		"""Create object using interface.Field instance"""
		return _InternalField(engine, field.name, field.value)

	def toField(self):
		"""Create interface.Field object from self"""
		return interface.Field(self.name, self.__value)

	def ancestors(self, include_self):
		"""
		Iterate through all ancestor fields
		Yields tuple (ancestor, last removed name part)
		"""
		name_copy = self.name[:]
		last = name_copy.pop() if not include_self else None
		while len(name_copy) > 0:
			yield _InternalField(self.__engine, name_copy), last
			last = name_copy.pop()

	def __getListColumnName(self, index):
		"""Get name of additional list column corresponding to given index"""
		return "c" + str(index)

	def isNull(self):
		"""Whether field contains Null value"""
		return (self.__value is None)

	def __get_type_str(self):
		"""Returns string with SQL type for stored value"""
		return self.__engine.getColumnType(self.__value) if not self.isNull() else None

	def __set_type_str(self, type_str):
		"""Set field type using given value from specification table"""
		if type_str is None:
			self.__value = None
		else:
			self.__value = self.__engine.getValueClass(type_str)()

	type_str = property(__get_type_str, __set_type_str)

	@property
	def type_str_as_value(self):
		"""Returns string with SQL type for stored value"""
		if not self.isNull():
			return self.__engine.getSafeValue(self.type_str)
		else:
			return self.__engine.getNullValue()

	@property
	def name_str_no_type(self):
		"""Returns name string with no type specifier"""
		return self.__engine.getNameString(['field'] + self.name)

	@property
	def safe_value(self):
		"""Returns value in form that can be safely used as value in queries"""
		return self.__engine.getSafeValue(self.__value)

	@property
	def name_str(self):
		"""Returns field name in string form"""
		return self.__engine.getNameString(['field', self.type_str] + self.name)

	@property
	def name_as_table(self):
		"""Returns field name in form that can be safely used as a table name"""
		return self.__engine.getSafeName(self.name_str)

	@property
	def name_as_value(self):
		"""Returns field name in form that can be safely used as value in queries"""
		return self.__engine.getSafeValue(self.name_str)

	@property
	def name_as_value_no_type(self):
		return self.__engine.getSafeValue(self.name_str_no_type)

	@property
	def columns_query(self):
		"""Returns string with additional values list necessary to query the value of this field"""
		numeric_columns = filter(lambda x: not isinstance(x, str), self.name)
		counter = 0
		l = []
		for column in numeric_columns:
			if column is None:
				l.append(self.__getListColumnName(counter))
			counter += 1

		# if value is null, this condition will be used alone,
		# so there's no need in leading comma
		return (('' if self.isNull() else ', ') + ', '.join(l) if len(l) > 0 else '')

	@property
	def columns_condition(self):
		"""Returns string with condition for operations on given field"""

		# do not skip Nones, because we need them for
		# getting proper index of list column
		numeric_columns = filter(lambda x: not isinstance(x, str), self.name)
		counter = 0
		l = []
		for column in numeric_columns:
			if column is not None:
				l.append(self.__getListColumnName(counter) +
					"=" + str(column))
			counter += 1

		return (' AND '.join([''] + l) if len(l) > 0 else '')

	def getDeterminedName(self, vals):
		"""Returns name with Nones filled with supplied list of values"""
		vals_copy = list(vals)
		func = lambda x: vals_copy.pop(0) if x is None else x
		return list(map(func, self.name))

	def getCreationStr(self, id_column, value_column, id_type, list_index_type):
		"""Returns string containing list of columns necessary to create field table"""
		counter = 0
		res = ""
		for elem in self.name:
			if not isinstance(elem, str):
				res += ", " + self.__getListColumnName(counter) + " " + list_index_type
				counter += 1

		return ("{id_column} {id_type}" +
			(", {value_column} {value_type}" if not self.isNull() else "") + res).format(
			id_column=id_column,
			value_column=value_column,
			id_type=id_type,
			value_type=self.type_str)

	@property
	def columns_values(self):
		"""Returns string with values of list columns that can be used in insertion"""
		res = ""
		for elem in self.name:
			if not isinstance(elem, str):
				res += ", " + str(elem)

		return res

	def __getListElements(self):
		"""Returns list of non-string name elements (i.e. corresponding to lists)"""
		return list(filter(lambda x: not isinstance(x, str), self.name))

	def pointsToListElement(self):
		"""Returns True if field points to element of the list"""
		return isinstance(self.name[-1], int)

	def getLastListColumn(self):
		"""Returns name and value of column corresponding to the last name element"""

		# This function makes sense only if self.pointsToListElement() is True
		if not self.pointsToListElement():
			raise interface.LogicError("Field should point to list element")

		list_elems = self.__getListElements()
		col_num = len(list_elems) - 1 # index of last column
		col_name = self.__getListColumnName(col_num)
		col_val = list_elems[col_num]
		return col_name, col_val

	@property
	def renumber_condition(self):
		"""Returns condition for renumbering after deletion of this element"""

		# This function makes sense only if self.pointsToListElement() is True
		if not self.pointsToListElement():
			raise interface.LogicError("Field should point to list element")

		self_copy = _InternalField(self.__engine, self.name)
		self_copy.name[-1] = None
		return self_copy.columns_condition

	@property
	def name_hashstr(self):
		"""
		Returns string that can serve as hash for field name along with its list elements
		"""
		name_copy = [repr(x) if x is not None else None for x in self.name]
		name_copy[-1] = None
		return self.__engine.getSafeValue(self.__engine.getNameString(name_copy))

	def __str__(self):
		return "IField (" + repr(self.name) + \
			(", value=" + repr(self.__value) if self.__value else "") + ")"

	def __repr__(self):
		return str(self)


class StructureLayer:
	"""Class which is connected to DB engine and incapsulates all SQL queries"""

	__ID_COLUMN = 'id' # name of column with object id in all tables

	# column names for specification table
	__FIELD_COLUMN = 'field' # field names
	__TYPE_COLUMN = 'type' # field types
	__REFCOUNT_COLUMN = 'refcount' # number of records with this type

	__MAX_COLUMN = 'max' # name of column with maximum list index values
	__VALUE_COLUMN = 'value' # name of column with field values


	def __init__(self, engine):
		self.engine = engine

		# memorize strings with support table names
		self.__ID_TABLE = self.engine.getSafeName(
			self.engine.getNameString(["id"]))

		self.__LISTSIZES_TABLE = self.engine.getSafeName(
			self.engine.getNameString(["listsizes"]))

		# types for support tables
		self.__ID_TYPE = self.engine.getIdType()
		self.__TEXT_TYPE = self.engine.getColumnType(str())
		self.__INT_TYPE = self.engine.getColumnType(int())

		# create support tables
		self.engine.begin()
		self.createSupportTables()
		self.engine.commit()


	def createSupportTables(self):
		"""Create database support tables (sort of caching)"""

		# create specification table, which holds field names, their types
		# and number of records of each type for all database objects
		self.engine.execute(("CREATE table IF NOT EXISTS {id_table} " +
			"({id_column} {id_type}, {field_column} {text_type}, " +
			"{type_column} {text_type}, " +
			"{refcount_column} {refcount_type})").format(
			field_column=self.__FIELD_COLUMN,
			id_column=self.__ID_COLUMN,
			id_table=self.__ID_TABLE,
			id_type=self.__ID_TYPE,
			refcount_column=self.__REFCOUNT_COLUMN,
			refcount_type=self.__INT_TYPE,
			text_type=self.__TEXT_TYPE,
			type_column=self.__TYPE_COLUMN))

		# create support table which holds maximum list index for each list
		# existing in database
		self.engine.execute(("CREATE table IF NOT EXISTS {listsizes_table} " +
			"({id_column} {id_type}, {field_column} {text_type}, " +
			"{max_column} {list_index_type})").format(
			field_column=self.__FIELD_COLUMN,
			id_column=self.__ID_COLUMN,
			id_type=self.__ID_TYPE,
			listsizes_table=self.__LISTSIZES_TABLE,
			list_index_type=self.__INT_TYPE,
			max_column=self.__MAX_COLUMN,
			text_type=self.__TEXT_TYPE))

	def deleteSpecification(self, id):
		"""Delete all information about object from specification table"""
		self.engine.execute("DELETE FROM {id_table} WHERE {id_column}={id}".format(
			id=id,
			id_table=self.__ID_TABLE,
			id_column=self.__ID_COLUMN))

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
			self.engine.execute("INSERT INTO {id_table} VALUES ({id}, {field_name}, {type}, 1)".format(
				field_name=field.name_as_value_no_type,
				id=id,
				id_table=self.__ID_TABLE,
				type=field.type_str_as_value))
		else:
		# otherwise increase the existing reference counter
			self.engine.execute(("UPDATE {id_table} SET {refcount_column}={refcount_column}+1 " +
				"WHERE {id_column}={id} AND {field_column}={field_name} " +
				"AND {type_column}={type}").format(
				field_column=self.__FIELD_COLUMN,
				field_name=field.name_as_value_no_type,
				id=id,
				id_column=self.__ID_COLUMN,
				id_table=self.__ID_TABLE,
				refcount_column=self.__REFCOUNT_COLUMN,
				type=field.type_str_as_value,
				type_column=self.__TYPE_COLUMN))

	def decreaseRefcount(self, id, field, num=1):
		"""
		Decrease reference count for given field and type
		one can specify a decrement if deleting values by mask

		field should have definite type
		"""

		# build condition for selecting necessary type
		# if type is Null, we should use ISNULL, because '=NULL' won't work
		if field.isNull():
			type_cond = self.__TYPE_COLUMN + ' ISNULL'
		else:
			type_cond = self.__TYPE_COLUMN + '=' + field.type_str_as_value

		# get current value of reference counter
		l = self.engine.execute(("SELECT {refcount_column} FROM {id_table} " +
			"WHERE {id_column}={id} AND {field_column}={field_name} AND {type_cond}").format(
			field_column=self.__FIELD_COLUMN,
			field_name=field.name_as_value_no_type,
			id=id,
			id_column=self.__ID_COLUMN,
			id_table=self.__ID_TABLE,
			refcount_column=self.__REFCOUNT_COLUMN,
			type_cond=type_cond))

		if l[0][0] < num:
		# if for some reason counter value is lower than expected, we will raise
		# exception, because this bug can be hard to catch later
			raise interface.StructureError("Unexpected value of reference counter: " + str(l[0][0]))
		if l[0][0] == num:
		# if these references are the last ones, delete this counter
			self.engine.execute(("DELETE FROM {id_table} " +
				"WHERE {id_column}={id} AND {field_column}={field_name} AND {type_cond}").format(
				field_column=self.__FIELD_COLUMN,
				field_name=field.name_as_value_no_type,
				id=id,
				id_column=self.__ID_COLUMN,
				id_table=self.__ID_TABLE,
				type_cond=type_cond))
		else:
		# otherwise just decrease the counter by given value
			self.engine.execute(("UPDATE {id_table} SET {refcount_column}={refcount_column}-{val} " +
				"WHERE {id_column}={id} AND {field_column}={field_name} " +
				"AND {type_column}={type}").format(
				field_column=self.__FIELD_COLUMN,
				field_name=field.name_as_value_no_type,
				id=id,
				id_column=self.__ID_COLUMN,
				id_table=self.__ID_TABLE,
				refcount_column=self.__REFCOUNT_COLUMN,
				type=field.type_str_as_value,
				type_column=self.__TYPE_COLUMN, val=num))

	def getValueTypes(self, id, field):
		"""Returns list of value types already stored in given field"""

		# just query specification table for all types for given object and field
		l = self.engine.execute(("SELECT {type_column} FROM {id_table} " +
			"WHERE {id_column}={id} AND {field_column}={field_name}").format(
			field_column=self.__FIELD_COLUMN,
			field_name=field.name_as_value_no_type,
			id=id,
			id_column=self.__ID_COLUMN,
			id_table=self.__ID_TABLE,
			type_column=self.__TYPE_COLUMN))

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
			regexp_val = self.engine.getSafeValue("^" + field.name_str_no_type +
				("." if exclude_self else ""))
			type = field.type_str_as_value
		else:
			regexp_cond = ""
			regexp_val = None
			type = None

		# Get list of fields
		l = self.engine.execute(("SELECT DISTINCT {field_column} FROM {id_table} " +
			"WHERE {id_column}={id}" + regexp_cond).format(
			field_column=self.__FIELD_COLUMN,
			id=id,
			id_column=self.__ID_COLUMN,
			id_table=self.__ID_TABLE,
			regexp=regexp_val,
			type=type,
			type_column=self.__TYPE_COLUMN))

		# fill the beginnings of found field names with the name of
		# given field (if any) or just construct result list
		res = []
		for elem in l:
			fld = _InternalField.fromNameStr(self.engine, elem[0])
			if field is not None:
				fld.name[:len(field.name)] = field.name
			res.append(fld)

		return res

	def objectExists(self, id):
		"""Check if object exists in database"""

		# We need just check if there is at least one row with its id
		# in specification table
		l = self.engine.execute("SELECT COUNT() FROM {id_table} WHERE {id_column}={id}".format(
			field_column=self.__FIELD_COLUMN,
			id=id,
			id_column=self.__ID_COLUMN,
			id_table=self.__ID_TABLE))

		return l[0][0] > 0


	def getFieldValue(self, id, field):
		"""
		Read value of given field(s)

		field should have definite type
		"""

		# if there is no such field - nothing to do
		if not self.engine.tableExists(field.name_str):
			return None

		# Get field values
		# If field is a mask (i.e., contains Nones), there will be more than one result
		l = self.engine.execute(("SELECT {value_column}{columns_query} FROM {field_name} " +
			"WHERE {id_column}={id}{columns_condition}").format(
			columns_condition=field.columns_condition,
			columns_query=field.columns_query,
			field_name=field.name_as_table,
			id=id,
			id_column=self.__ID_COLUMN,
			value_column=self.__VALUE_COLUMN if not field.isNull() else ""))

		# Convert results to list of _InternalFields
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

			res.append(_InternalField(self.engine,
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

			self.engine.execute(("UPDATE {listsizes_table} " +
				"SET {max_column}={val} " +
				"WHERE {id_column}={id} AND {field_column}={field_name}").format(
				field_column=self.__FIELD_COLUMN,
				field_name=field.name_hashstr,
				id=id,
				id_column=self.__ID_COLUMN,
				listsizes_table=self.__LISTSIZES_TABLE,
				max_column=self.__MAX_COLUMN,
				val=val))
		else:
		# create new record
			self.engine.execute(("INSERT INTO {listsizes_table} " +
				"VALUES ({id}, {field_name}, {val})").format(
				field_name=field.name_hashstr,
				id=id,
				listsizes_table=self.__LISTSIZES_TABLE,
				val=val))

	def assureFieldTableExists(self, field):
		"""
		Create table for storing values of this field if it does not exist yet

		field should have definite type
		"""

		# Create table
		self.engine.execute("CREATE TABLE IF NOT EXISTS {field_name} ({values_str})"
			.format(field_name=field.name_as_table,
			values_str=field.getCreationStr(self.__ID_COLUMN,
				self.__VALUE_COLUMN, self.__ID_TYPE, self.__INT_TYPE)))

	def getMaxListIndex(self, id, field):
		"""Get maximum value of list index for the undefined column of the field"""

		l = self.engine.execute(("SELECT {max_column} FROM {listsizes_table} " +
			"WHERE {id_column}={id} AND {field_column}={field_name}").format(
			field_column=self.__FIELD_COLUMN,
			field_name=field.name_hashstr,
			id=id,
			id_column=self.__ID_COLUMN,
			listsizes_table=self.__LISTSIZES_TABLE,
			max_column=self.__MAX_COLUMN))

		if len(l) > 0:
			return l[0][0]
		else:
			return None

	def buildSqlQuery(self, condition):
		"""Recursive function to transform condition into SQL query"""

		if not condition.leaf:
			# child conditions
			cond1 = self.buildSqlQuery(condition.operand1)
			cond2 = self.buildSqlQuery(condition.operand2)

			# mapping to SQL operations
			operations = {
				interface.SearchRequest.AND: 'INTERSECT',
				interface.SearchRequest.OR: 'UNION'
			}

			return ("SELECT * FROM ({cond1}) {operation} SELECT * FROM ({cond2})"
				.format(cond1=cond1, cond2=cond2,
				operation=operations[condition.operator]))

		# Leaf condition
		op1 = condition.operand1 # it must be Field
		op2 = condition.operand2 # it must be some value

		# set proper type for the field
		if op2 is not None:
			op1.type_str = self.engine.getColumnType(op2)
		else:
			op1.type_str = None

		# If table with given field does not exist, just return empty query
		if not self.engine.tableExists(op1.name_str):
			return self.engine.getEmptyCondition()

		safe_name = condition.operand1.name_as_table
		not_str = " NOT " if condition.invert else " "

		# mapping to SQL comparisons
		comparisons = {
			interface.SearchRequest.EQ: '=',
			interface.SearchRequest.REGEXP: 'REGEXP',
			interface.SearchRequest.LT: '<',
			interface.SearchRequest.GT: '>',
			interface.SearchRequest.LTE: '<=',
			interface.SearchRequest.GTE: '>='
		}

		# build query

		if op2 is not None or not condition.invert:

			# construct comparing condition
			if op2 is not None:
				comp_str = "WHERE{not_str} {value_column} {comp} {val}".format(
					comp=comparisons[condition.operator],
					not_str=not_str,
					val=self.engine.getSafeValue(op2),
					value_column=self.__VALUE_COLUMN)
			else:
				comp_str = ""

			# construct query
			result = ("SELECT DISTINCT {id_column} FROM {field_name} " +
				"{comp_str}{columns_condition}").format(
				columns_condition=op1.columns_condition,
				comp_str=comp_str,
				field_name=safe_name,
				id_column=self.__ID_COLUMN)

			if condition.invert:
			# we will add objects that do not even have such field
				result += " UNION "
		else:
			result = ""

		# if we need to invert results, we have to add all objects that do
		# not have this field explicitly, because they won't be caught by previous query
		if condition.invert:
			result += ("SELECT * FROM (SELECT {id_column} FROM {id_table} " +
				"EXCEPT SELECT {id_column} FROM {field_name})").format(
				field_name=safe_name,
				id_column=self.__ID_COLUMN,
				id_table=self.__ID_TABLE)

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
			self.engine.execute(("UPDATE {field_name} " +
				"SET {col_name}={col_name}+{shift} " +
				"WHERE {id_column}={id}{cond} AND {col_name}>={col_val}").format(
				col_name=col_name,
				col_val=col_val,
				cond=cond,
				field_name=field.name_as_table,
				id=id,
				id_column=self.__ID_COLUMN,
				shift=shift))

	def deleteValues(self, id, field, condition=None):
		"""Delete value of given field(s)"""

		field_copy = _InternalField(self.engine, field.name)

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
				id_column=self.__ID_COLUMN)

			# get number of records to be deleted
			res = self.engine.execute("SELECT COUNT() " + query_str)
			del_num = res[0][0]

			# if there are any, delete them
			if del_num > 0:
				self.decreaseRefcount(id, field_copy, num=del_num)
				self.engine.execute("DELETE " + query_str)

				# check if the table is empty and if it is - delete it too
				if self.engine.tableIsEmpty(field_copy.name_str):
					self.engine.deleteTable(field_copy.name_str)

	def addValueRecord(self, id, field):
		"""
		Add value to the corresponding table

		field should have definite type
		"""

		new_value = (", " + field.safe_value) if not field.isNull() else ""
		self.engine.execute(("INSERT INTO {field_name} " +
			"VALUES ({id}{value}{columns_values})").format(
			columns_values=field.columns_values,
			field_name=field.name_as_table,
			id=id,
			value=new_value))


class LogicLayer:
	"""Class, representing DDB logic"""

	def __init__(self, engine, structure):
		self.engine = engine
		self.structure = structure

	def deleteField(self, id, field):
		"""Delete given field(s)"""

		if field.pointsToListElement():
			# deletion of list element requires renumbering of other elements
			self.renumber(id, field, -1)
		else:
			# otherwise just delete values using given field mask
			self.structure.deleteValues(id, field)

	def removeConflicts(self, id, field):
		"""
		Check that adding this field does not break the database structure, namely:
		given field can either contain value, or list, or map, not several at once
		"""

		# check all ancestor fields in hierarchy
		for anc, last in field.ancestors(include_self=False):

			# delete all values whose names are a part of the name of field to add
			# in other words, no named maps or lists
			types = self.structure.getValueTypes(id, anc)
			for type in types:
				anc.type_str = type
				self.deleteField(id, anc)

			self.checkForListAndMapConflicts(id, anc, last)

		# check separately for root level lists and maps
		self.checkForListAndMapConflicts(id, None, field.name[0])

	def checkForListAndMapConflicts(self, id, field, last):
		"""
		Check that adding this field will not mean adding list elements to map
		or map keys to list. If field is None, root level is checked.
		"""

		# Get all fields with names, starting from name_copy, excluding
		# the one whose name equals name_copy
		relatives = self.structure.getFieldsList(id, field, exclude_self=True)

		# we have to check only first field in list
		# if there are no conflicts, other fields do not conflict too
		if len(relatives) > 0:
			elem = relatives[0].name[len(field.name) if field is not None else 0]

			if isinstance(last, str) and not isinstance(elem, str):
				raise interface.StructureError("Cannot modify map, when list already exists on this level")
			if not isinstance(last, str) and isinstance(elem, str):
				raise interface.StructureError("Cannot modify list, when map already exists on this level")


	def addFieldToSpecification(self, id, field):
		"""Check if field conforms to hierarchy and if yes, add it"""

		# check if there are already field with this name in object
		types = self.structure.getValueTypes(id, field)

		# if adding a new field, ensure that there will be
		# no conflicts in database structure
		if len(types) == 0:
			self.removeConflicts(id, field)

		self.structure.increaseRefcount(id, field, new_type=(not field.type_str in types))

	def setFieldValue(self, id, field):
		"""Set value of given field"""

		# Update maximum values cache
		for anc, last in field.ancestors(include_self=True):
			if anc.pointsToListElement():
				self.structure.updateListSize(id, anc)

		# Delete old value (checking all tables because type could be different)
		self.structure.deleteValues(id, field)

		# Create field table if it does not exist yet
		self.structure.assureFieldTableExists(field)
		self.addFieldToSpecification(id, field) # create object header
		self.structure.addValueRecord(id, field) # Insert new value

	def deleteObject(self, id):
		"""Delete object with given ID"""

		fields = self.structure.getFieldsList(id)

		# for each field, remove it from tables
		for field in fields:
			self.deleteField(id, field)

		self.structure.deleteSpecification(id)

	def renumber(self, id, target_field, shift):
		"""Renumber list elements before insertion or deletion"""

		# Get all child field names
		fields_to_reenum = self.structure.getFieldsList(id, target_field)
		for fld in fields_to_reenum:

			# if shift is negative, we should delete elements first
			if shift < 0:
				self.structure.deleteValues(id, fld, target_field.columns_condition)

			# shift numbers of all elements in list
			self.structure.renumberList(id, target_field, fld, shift)

	def processModifyRequest(self, id, fields):

		if id is None:
			new_obj = True
			id = self.engine.getNewId()
		else:
			new_obj = False

		for field in fields:
			self.setFieldValue(id, field)

		if new_obj:
			return id

	def processDeleteRequest(self, id, fields):

		if fields is not None:
			# remove specified fields
			for field in fields:
				self.deleteField(id, field)
			return
		else:
			# delete whole object
			self.deleteObject(id)

	def processReadRequest(self, id, fields):

		# check if object exists first
		if not self.structure.objectExists(id):
			raise interface.LogicError("Object " + str(id) + " does not exist")

		# if list of fields was not given, read all object's fields
		if fields is None:
			fields = self.structure.getFieldsList(id)
		else:
			res = []
			for field in fields:
				res += self.structure.getFieldsList(id, field)
			fields = res

		result_list = []
		for field in fields:
			for type in self.structure.getValueTypes(id, field):
				field.type_str = type
				res = self.structure.getFieldValue(id, field)
				if res is not None:
					result_list += res

		return [x.toField() for x in result_list]

	def processSearchRequest(self, condition):
		"""Search for all objects using given search condition"""

		request = self.structure.buildSqlQuery(condition)
		result = self.engine.execute(request)
		list_res = [x[0] for x in result]

		return list_res

	def processInsertRequest(self, id, target_field, fields, insert_many=False):

		def enumerate(fields_list, col_num, starting_num, insert_many):
			"""Enumerate given column in list of fields"""
			counter = starting_num
			for field in fields_list:
				# FIXME: Hide .name usage in _InternalField
				if insert_many:
					for fld in field:
						fld.name[col_num] = counter
					counter += 1
				else:
					field.name[col_num] = counter

		# FIXME: Hide .name usage in _InternalField
		target_col = len(target_field.name) - 1 # last column in name of target field

		max = self.structure.getMaxListIndex(id, target_field)
		if max is None:
		# list does not exist yet
			enumerate(fields, target_col, 0, insert_many)
			if insert_many:
				temp = []
				for flds in fields:
					temp += flds
				fields = temp
		# FIXME: Hide .name usage in _InternalField
		elif target_field.name[target_col] is None:
		# list exists and we are inserting elements to the end
			starting_num = max + 1
			enumerate(fields, target_col, starting_num, insert_many)
			if insert_many:
				temp = []
				for flds in fields:
					temp += flds
				fields = temp
		else:
		# list exists and we are inserting elements to the beginning or to the middle
			self.renumber(id, target_field,
				(1 if not insert_many else len(fields)))
			# FIXME: Hide .name usage in _InternalField
			enumerate(fields, target_col, target_field.name[target_col], insert_many)
			if insert_many:
				temp = []
				for flds in fields:
					temp += flds
				fields = temp

		self.processModifyRequest(id, fields)


class SimpleDatabase(interface.Database):
	"""Class, representing DDB request handler"""

	def __init__(self, engine_class, path=None, open_existing=None):
		if not issubclass(engine_class, interface.Engine):
			raise interface.LogicError("Engine class must be derived from Engine interface")
		self.engine = engine_class(path, open_existing)
		self.structure = StructureLayer(self.engine)
		self.logic = LogicLayer(self.engine, self.structure)

	def disconnect(self):
		self.engine.disconnect()

	def prepareRequest(self, request):
		"""Prepare request for processing"""

		def convertFields(fields, engine):
			"""Convert given fields list to _InternalFields list"""
			if fields is not None:
				return [_InternalField.fromField(engine, x) for x in fields]
			else:
				return None

		def convertCondition(condition, engine):
			"""Convert fields in given condition to _InternalFields"""
			if condition.leaf:
				condition.operand1 = _InternalField.fromField(
					engine, condition.operand1)
			else:
				convertCondition(condition.operand1, engine)
				convertCondition(condition.operand2, engine)

		def propagateInversion(condition):
			"""Propagate inversion flags to the leafs of condition tree"""

			if not condition.leaf:
				if condition.invert:

					condition.invert = False

					condition.operand1.invert = not condition.operand1.invert
					condition.operand2.invert = not condition.operand2.invert

					if condition.operator == interface.SearchRequest.AND:
						condition.operator = interface.SearchRequest.OR
					elif condition.operator == interface.SearchRequest.OR:
						condition.operator = interface.SearchRequest.AND

				propagateInversion(condition.operand1)
				propagateInversion(condition.operand2)

		# Convert Fields
		if hasattr(request, 'fields'):
			converted_fields = convertFields(request.fields, self.engine)

		if hasattr(request, 'field_groups'):
			converted_groups = [convertFields(fields, self.engine) for fields in request.field_groups]

		if hasattr(request, 'condition'):
			converted_condition = copy.deepcopy(request.condition)
			convertCondition(converted_condition, self.engine)

		if hasattr(request, 'target_field'):
			converted_target = _InternalField.fromField(
				self.engine, request.target_field)

		# Prepare handler function and parameters list
		# (so that we do not have to do it inside a transaction)
		if isinstance(request, interface.ModifyRequest):
			params = (request.id, converted_fields)
			handler = self.logic.processModifyRequest
		elif isinstance(request, interface.ReadRequest):
			params = (request.id, converted_fields)
			handler = self.logic.processReadRequest
		elif isinstance(request, interface.InsertRequest):

			# fields to insert have relative names
			for field in converted_fields:
				field.name = request.target_field.name + field.name

			params = (request.id, converted_target, converted_fields)
			handler = self.logic.processInsertRequest
		elif isinstance(request, interface.InsertManyRequest):

			# fields to insert have relative names
			for field in converted_groups:
				for fld in field:
					fld.name = request.target_field.name + fld.name

			params = (request.id, converted_target,	converted_groups, True)
			handler = self.logic.processInsertRequest
		elif isinstance(request, interface.DeleteRequest):
			params = (request.id, converted_fields)
			handler = self.logic.processDeleteRequest
		elif isinstance(request, interface.SearchRequest):
			propagateInversion(converted_condition)
			params = (converted_condition,)
			handler = self.logic.processSearchRequest
		else:
			raise interface.FormatError("Unknown request type: " + request.__class__.__name__)

		return handler, params

	def processRequests(self, requests):
		"""Start/stop transaction, handle exceptions"""

		prepared_requests = [self.prepareRequest(x) for x in requests]

		# Handle request inside a transaction
		res = []
		self.engine.begin()
		try:
			for handler, params in prepared_requests:
				res.append(handler(*params))
		except:
			self.engine.rollback()
			raise
		self.engine.commit()
		return res

	def processRequest(self, request):
		"""Process a single request"""
		return self.processRequests([request])[0]
