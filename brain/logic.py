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
	_VALUE_COLUMN = 'value' # name of column with field values


	def __init__(self, engine):
		self._engine = engine

		# memorize strings with support table names
		self._ID_TABLE = self._engine.getNameString(["id"])

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
		id_table_spec = ("({id_column} {id_type}, {field_column} {text_type}, " +
			"{type_column} {text_type}, " +
			"{refcount_column} {refcount_type})").format(
			field_column=self._FIELD_COLUMN,
			id_column=self._ID_COLUMN,
			id_type=self._ID_TYPE,
			refcount_column=self._REFCOUNT_COLUMN,
			refcount_type=self._INT_TYPE,
			text_type=self._TEXT_TYPE,
			type_column=self._TYPE_COLUMN)

		if not self._engine.tableExists(self._ID_TABLE):
			self._engine.execute("CREATE table {} " + id_table_spec, [self._ID_TABLE])

	def _deleteSupportTables(self):
		self._engine.deleteTable(self._ID_TABLE)

	def repairSupportTables(self):
		"""Recreate support tables according to other information in DB"""

		# if the repair request was created, we cannot rely on existing tables
		self._deleteSupportTables()
		self._createSupportTables()

		# search for all field tables in database
		tables = self._engine.getTablesList()

		# we need to filter out non-field tables
		tables.remove(self._ID_TABLE)
		tables = [x for x in tables if Field.isFieldTableName(self._engine, x)]

		# collect info from field tables and create refcounters table in memory
		refcounters = {}
		for table in tables:
			rows = self._engine.execute("SELECT id FROM {}", [table])
			ids = [x[0] for x in rows]
			field = Field.fromTableName(self._engine, table)
			name_str = field.name_str
			type_str = field.type_str

			for obj_id in ids:
				if obj_id not in refcounters:
					refcounters[obj_id] = {}

				if name_str not in refcounters[obj_id]:
					refcounters[obj_id][name_str] = {}

				if type_str not in refcounters[obj_id][name_str]:
					refcounters[obj_id][name_str][type_str] = 1
				else:
					refcounters[obj_id][name_str][type_str] += 1

		# flatten refcounters hierarchy
		values = []
		for obj_id in refcounters:
			for name_str in refcounters[obj_id]:
				for type_str in refcounters[obj_id][name_str]:
					values.append([obj_id, name_str, type_str,
						refcounters[obj_id][name_str][type_str]])

		# fill refcounters table
		for values_list in values:
			self._engine.execute("INSERT INTO {} VALUES (?, ?, ?, ?)",
				[self._ID_TABLE], values_list)

	def updateRefcounts(self, id, to_delete, to_add):
		"""
		Update reference counters, first deleting and then creating entries.
		to_delete - list of tuples (path string, type string)
		to_add - list of tuples (path string, type string, value)
		"""

		# delete old refcounts
		if len(to_delete) > 0:
			delete_strings = ["(" + self._FIELD_COLUMN + "=? AND " +
				self._TYPE_COLUMN + "=?)"] * len(to_delete)
			delete_condition = "(" + " OR ".join(delete_strings) + ")"
			delete_query_values = [id]

			for name_str, type_str in to_delete:
				delete_query_values += [name_str, type_str]

			self._engine.execute("DELETE FROM {} WHERE " +
				self._ID_COLUMN + "=? AND " + delete_condition,
				[self._ID_TABLE], delete_query_values)

		# add new refcounts
		if len(to_add) > 0:
			add_values = []
			for name_str, type_str, refcount in to_add:
				add_values.append([id, name_str, type_str, refcount])
			self._engine.insertMany(self._ID_TABLE, add_values)

	def _getValueTypes(self, id, field):
		"""Returns list of value types already stored in given field"""

		# just query specification table for all types for given object and field
		rows = self._engine.execute("SELECT " + self._TYPE_COLUMN + " FROM {} " +
			"WHERE " + self._ID_COLUMN + "=? AND " + self._FIELD_COLUMN + "=?",
			[self._ID_TABLE], [id, field.name_str])

		return [type_str for type_str, in rows]

	def _getAncestorsValueTypes(self, id, field):
		"""
		Returns possible types for field ancestors (not including the field itself)
		in form of {name_str: [type_str, ...], ...}
		"""
		ancestors = field.getAncestors()
		ancestors.pop() # remove last element, which is equal to 'field'

		values = [id] + [ancestor.name_str for ancestor in ancestors]
		queries = [self._FIELD_COLUMN + "=?"] * len(ancestors)
		query = ("AND (" + " OR ".join(queries) + ")") if len(queries) > 0 else ""

		rows = self._engine.execute("SELECT " + self._FIELD_COLUMN +
			", " + self._TYPE_COLUMN + " FROM {} " +
			" WHERE " + self._ID_COLUMN + "=? " + query,
			[self._ID_TABLE], values)

		result = {}
		for name_str, type_str in rows:
			if name_str not in result:
				result[name_str] = []
			result[name_str].append(type_str)

		return result

	def getFirstConflict(self, id, field):
		"""
		Find field, pointing to first (highest in the hierarchy)
		path, conflicting with given field.
		Possible conflicts:
		- storing list or map in place of value
		- storing list in place of map
		- storing map in place of list

		Returns tuple (field object for conflict, name strings for each node of
		path leading to the conflict)
		"""

		ancestors_types = self._getAncestorsValueTypes(id, field)

		# we will store field objects for all hierarchy leading to conflict here
		existing_hierarchy = []

		for name_str in sorted(ancestors_types):
			ancestor = Field.fromNameStr(self._engine, name_str)
			ancestor.fillListIndexesFromField(field)

			new_structure_type = dict if isinstance(field.name[len(ancestor.name)], str) else list

			# for each possible ancestor type, get ancestor value
			for type_str in ancestors_types[name_str]:
				ancestor.type_str = type_str

				rows = self._engine.execute("SELECT " + self._VALUE_COLUMN +
					" FROM {} WHERE " + self._ID_COLUMN + "=? " +
					ancestor.list_indexes_condition,
					[ancestor.table_name], [id])

				if len(rows) > 0:
					ancestor.db_value, = rows[0]
					existing_value = type(ancestor.py_value)

					# there is no conflict, if we want to add/change key in map
					# or add/change index in list; otherwise there is a conflict
					conflict = not ((existing_value == list and new_structure_type == list) or
						(existing_value == dict and new_structure_type == dict))

					if conflict:
						return ancestor, existing_hierarchy

			existing_hierarchy.append(ancestor.name_str)

		return None, existing_hierarchy

	def getRefcounts(self, id, name_type_pairs):

		elem = "(" + self._FIELD_COLUMN + "=? AND " + self._TYPE_COLUMN + "=?)"
		cond = " OR ".join([elem] * len(name_type_pairs))

		values = [id]
		for name_str, type_str in name_type_pairs:
			values += [name_str, type_str]

		l = self._engine.execute("SELECT " + self._FIELD_COLUMN + ", " + self._TYPE_COLUMN +
			", " + self._REFCOUNT_COLUMN +
			" FROM {} WHERE " + self._ID_COLUMN + "=? AND (" + cond + ")",
			[self._ID_TABLE], values)

		return {(name_str, type_str): refcount for name_str, type_str, refcount in l}

	def getRawFieldsInfo(self, id, masks=None, include_refcounts=False):

		regexp_vals = []
		if masks is not None:
		# If fields list is given, return only fields, which contain its name in the beginning
			regexp_cond_list = []
			for mask in masks:
				regexp_cond_list.append(self._FIELD_COLUMN +
					" " + self._engine.getRegexpOp() + " ?")
				regexp_vals.append("^" + re.escape(mask.name_str) + "(\.\.|$)")
			regexp_cond = " AND (" + " OR ".join(regexp_cond_list) + ")"
		else:
			regexp_cond = ""

		# Get information
		l = self._engine.execute("SELECT " + self._FIELD_COLUMN + ", " + self._TYPE_COLUMN +
			((", " + self._REFCOUNT_COLUMN) if include_refcounts else "") +
			" FROM {} WHERE " + self._ID_COLUMN + "=?" + regexp_cond,
			[self._ID_TABLE], [id] + regexp_vals)

		# fill 'raw' information map - name strings to existing types correspondence
		raw_fields_info = {}
		for elem in l:
			name_str = elem[0]
			type_str = elem[1]
			if name_str not in raw_fields_info:
				raw_fields_info[name_str] = []

			if include_refcounts:
				to_append = (type_str, elem[2])
			else:
				to_append = (type_str, None)

			raw_fields_info[name_str].append(to_append)

		return raw_fields_info

	def getFieldsInfo(self, id, masks=None, include_refcounts=False):
		"""
		Get types of given fields from support table
		if masks is equal to None, info for all object's fields is returned
		"""

		raw_fields_info = self.getRawFieldsInfo(id, masks, include_refcounts=include_refcounts)
		res = {}

		# construct resulting list of partially defined fields
		for name_str in raw_fields_info:

			name_str_map = {}

			if masks is None:
			# just construct all possible fields from name strings
				for type_str, refcount in raw_fields_info[name_str]:
					f = Field.fromNameStr(self._engine, name_str)
					f.type_str = type_str
					name_str_map[type_str] = ([f], refcount)
			else:
			# since we do not know, which name string was found using which mask,
			# we should skip unmatched combinations of masks and name strings
				temp = Field.fromNameStr(self._engine, name_str)
				for mask in masks:
					if temp.matches(mask):
						for type_str, refcount in raw_fields_info[name_str]:

							if type_str not in name_str_map:
								name_str_map[type_str] = ([], refcount)

							f = Field.fromNameStr(self._engine, name_str)
							f.name[:len(mask.name)] = mask.name
							f.type_str = type_str
							name_str_map[type_str][0].append(f)

			res[name_str] = name_str_map

		return res

	def getFlatFieldsInfo(self, id, masks=None):
		fields_info = self.getFieldsInfo(id, masks)

		res = []
		for name_str in fields_info:
			for type_str in fields_info[name_str]:
				fields, refcount = fields_info[name_str][type_str]
				res += fields
		return res

	def objectExists(self, id):
		"""Check if object exists in database"""

		# We need just check if there is at least one row with its id
		# in specification table
		l = self._engine.execute("SELECT COUNT(*) FROM {} WHERE " + self._ID_COLUMN + "=?",
			[self._ID_TABLE], [id])

		return l[0][0] > 0

	def getFieldValues(self, id, fields):
		"""
		Read value of given fields

		Fields should have definite types and column indexes
		Field tables are assumed to exist
		"""

		# we need to get values of each type separately, because some databases
		# have strict type checks
		sorted_by_type = {}
		for field in fields:
			if field.type_str not in sorted_by_type:
				sorted_by_type[field.type_str] = []

			sorted_by_type[field.type_str].append(field)

		result = []
		for typed_fields in sorted_by_type.values():
			result += self._getFieldValuesSameType(id, typed_fields)
		return result


	def _getFieldValuesSameType(self, id, fields):

		queries = []
		values = []
		tables = []
		max_list_indexes = 0
		for field in fields:
			num = field.list_indexes_number
			if num > max_list_indexes:
				max_list_indexes = num

		for i, field in enumerate(fields):

			num = field.list_indexes_number
			if num < max_list_indexes:
				stub_columns = ", " + ", ".join(["0"] * (max_list_indexes - num))
			else:
				stub_columns = ""

			query = "SELECT " + str(i) + ", " + self._VALUE_COLUMN + field.list_indexes_query + stub_columns + \
				" FROM {} WHERE " + self._ID_COLUMN + "=?" + field.list_indexes_condition

			values.append(id)
			tables.append(field.table_name)
			queries.append(query)

		l = self._engine.execute(" UNION ".join(queries), tables, values)

		res = []
		for elem in l:

			elem = tuple(elem) # get rid of weird engine-specific row wrappers

			index = elem[0]
			num = fields[index].list_indexes_number

			value = elem[1]
			list_indexes = elem[2:num + 2]

			new_field = Field(self._engine, fields[index].name)
			new_field.fillListIndexes(list_indexes)
			new_field.type_str = fields[index].type_str
			new_field.db_value = value
			res.append(new_field)

		return res

	def ensureTablesExist(self, fields):

		elem = "(" + self._FIELD_COLUMN + "=? AND " + self._TYPE_COLUMN + "=?)"
		cond = " OR ".join([elem] * len(fields))

		values = []
		tables = {(field.name_str, field.type_str): field for field in fields}
		for name_str, type_str in tables:
			values += [name_str, type_str]

		l = self._engine.execute("SELECT " + self._FIELD_COLUMN + ", " + self._TYPE_COLUMN +
			" FROM {} WHERE " + cond, [self._ID_TABLE], values)

		for name_str, type_str in l:
			if (name_str, type_str) in tables:
				del tables[(name_str, type_str)]

		for field in tables.values():
			self._createFieldTable(field)

	def _createFieldTable(self, field):
		"""
		Create table for storing values of this field if it does not exist yet

		field should have definite type
		"""

		table_spec = field.getCreationStr(self._ID_COLUMN,
			self._VALUE_COLUMN, self._ID_TYPE, self._INT_TYPE)
		self._engine.execute("CREATE TABLE {} (" + table_spec + ")",
			[field.table_name])

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

			if cond1 is None and cond2 is None:
				return None, None, None
			elif cond1 is None:
				if condition.operator == op.AND:
					return None, None, None
				elif condition.operator == op.OR:
					return cond2, tables2, values2
			elif cond2 is None:
				if condition.operator == op.AND:
					return None, None, None
				elif condition.operator == op.OR:
					return cond1, tables1, values1

			return "SELECT * FROM (" + cond1 + ") as temp " + \
				operations[condition.operator] + " SELECT * FROM (" + \
				cond2 + ") as temp", tables1 + tables2, values1 + values2

		# Leaf condition
		op1 = condition.operand1 # it must be Field without value
		op2 = condition.operand2 # it must be Field with value

		# If table with given field does not exist, just return empty query
		if op1 is None:
			if condition.invert:
				return "SELECT DISTINCT " + self._ID_COLUMN + " FROM {}", \
					[self._ID_TABLE], []
			else:
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

		# construct comparing condition
		comp_str = "WHERE " + not_str + " " + self._VALUE_COLUMN + " " + \
			comparisons[condition.operator] + " ?"
		values = [op2.db_value]

		# construct query
		result = "SELECT DISTINCT " + self._ID_COLUMN + " FROM {} " + \
			comp_str + " " + op1.list_indexes_condition
		tables = [condition.operand1.table_name]

		if condition.invert:
		# we will add objects that do not even have such field
			result += " UNION "

		# if we need to invert results, we have to add all objects that do
		# not have this field explicitly, because they won't be caught by previous query
		if condition.invert:
			result += "SELECT " + self._ID_COLUMN + " FROM " + \
				"(SELECT DISTINCT " + self._ID_COLUMN + " FROM {} " + \
				"EXCEPT SELECT DISTINCT " + self._ID_COLUMN + " FROM {}) as temp"
			tables += [self._ID_TABLE, condition.operand1.table_name]

		return result, tables, values

	def renumberLists(self, id, field, shift):

		# Get the name and the value of last numerical column
		col_name, col_val = field.getLastListColumn()
		cond = field.renumber_condition

		# Get all child field names
		fields_to_reenum = self.getFlatFieldsInfo(id, [field])

		for fld in fields_to_reenum:

			self._engine.execute("UPDATE {} " +
				"SET " + col_name + "=" + col_name + "+? " +
				"WHERE " + self._ID_COLUMN + "=?" + cond +
				" AND " + col_name + ">=?",
				[fld.table_name], [shift, id, col_val])

	def addValueRecords(self, id, fields):
		"""All fields must have the same path and type"""

		values = [[id] + field.value_record for field in fields]
		self._engine.insertMany(fields[0].table_name, values)

	def getMaxListIndex(self, id, field):
		"""Get maximum index in list, specified by given field"""

		col_name, col_val = field.getLastListColumn()
		cond = field.renumber_condition

		max = -1
		for fld in self.getFlatFieldsInfo(id, [field]):
			res = self._engine.execute("SELECT MAX(" + col_name + ") FROM {} WHERE " +
				self._ID_COLUMN + "=?" + cond, [fld.table_name], [id])

			if res[0][0] is not None and res[0][0] > max:
				max = res[0][0]

		return max if max != -1 else None

	def objectHasField(self, id, field):
		"""Returns True if object has given field (with any type of value)"""

		# if field points to root, object definitely has it
		if len(field.name) == 0:
			return True

		types = self._getValueTypes(id, field)
		if len(types) == 0:
			return False

		field_copy = Field(self._engine, field.name)
		queries = []
		tables = []
		values = []
		list_indexes_condition = field_copy.list_indexes_condition
		for type in types:
			field_copy.type_str = type
			queries.append("SELECT " + self._ID_COLUMN +
				" FROM {} WHERE " + self._ID_COLUMN + "=?" +
				list_indexes_condition)
			tables.append(field_copy.table_name)
			values.append(id)
		query = "SELECT COUNT(*) FROM (" + " UNION ".join(queries) + ") AS temp"

		res = self._engine.execute(query, tables, values)

		return res[0][0] > 0

	def deleteFields(self, id, masks=None):
		"""Delete object with given ID"""

		fields_info = self.getFieldsInfo(id, masks, include_refcounts=True)

		to_delete = []
		to_add = []
		for name_str in fields_info:
			for type_str in fields_info[name_str]:

				fields, refcount = fields_info[name_str][type_str]

				# prepare query for deletion
				conditions = []
				for field in fields:
					cond = field.raw_list_indexes_condition
					if cond != "":
						conditions.append(cond)

				if len(conditions) > 0:
					condition_str = " AND (" + " OR ".join(conditions) + ")"
				else:
					condition_str = ""

				query_str = "FROM {} WHERE " + self._ID_COLUMN + "=? " + condition_str
				tables = [field.table_name]
				values = [id]

				# get number of records to be deleted
				res = self._engine.execute("SELECT COUNT(*) " + query_str, tables,values)
				del_num = res[0][0]

				# if there are any, delete them
				if del_num > 0:
					typed_name_str = field.table_name

					self._engine.execute("DELETE " + query_str, tables, values)

					# check if the table is empty and if it is - delete it too
					if self._engine.tableIsEmpty(typed_name_str):
						self._engine.deleteTable(typed_name_str)

					if del_num == refcount:
						to_delete.append((name_str, type_str))
					else:
						to_delete.append((name_str, type_str))
						to_add.append((name_str, type_str, refcount - del_num))

		self.updateRefcounts(id, to_delete, to_add)


class LogicLayer:
	"""Class, representing DDB logic"""

	def __init__(self, engine):
		self._engine = engine
		self._structure = _StructureLayer(engine)

	def _checkForConflicts(self, id, field, remove_conflicts):
		"""
		Check that adding this field does not break the database structure, namely:
		given field can either contain value, or list, or map, not several at once
		"""

		conflict, existing_hierarchy = self._structure.getFirstConflict(id, field)

		if conflict is not None:
			if remove_conflicts:
				self._structure.deleteFields(id, [conflict])
			else:
				raise interface.StructureError("Path " +
					repr(conflict.name + [field.name[len(conflict.name)]]) +
					" conflicts with existing structure")

		hierarchy = []
		for i in range(0, len(field.name)):
			fld = Field(self._engine, field.name[:i])
			if fld.name_str not in existing_hierarchy:
				fld.py_value = dict() if isinstance(field.name[i], str) else list()
				hierarchy.append(fld)
		return hierarchy

	def _setFieldValues(self, id, fields):

		sorted = {}
		for field in fields:
			key = (field.name_str, field.type_str)
			if key not in sorted:
				sorted[key] = []

			sorted[key].append(field)

		refcounts = self._structure.getRefcounts(id, sorted.keys())

		refcounts_to_delete = []
		refcounts_to_add = []
		tables_to_create = []
		for name_type_pair in sorted:
			if name_type_pair in refcounts:
				refcounts_to_delete.append(name_type_pair)
				existing_refcount = refcounts[name_type_pair]
			else:
				tables_to_create.append(sorted[name_type_pair][0])
				existing_refcount = 0

			name_str, type_str = name_type_pair
			refcounts_to_add.append((name_str, type_str,
				existing_refcount + len(sorted[name_type_pair])))

		if len(tables_to_create) > 0:
			self._structure.ensureTablesExist(tables_to_create)

		self._structure.updateRefcounts(id, refcounts_to_delete, refcounts_to_add)
		for key in sorted:
			self._structure.addValueRecords(id, sorted[key])

	def _fillWithNones(self, id, path):
		max = self._structure.getMaxListIndex(id, path)
		if max is None:
			start = 0
		else:
			start = max + 1
		end = path.name[-1]

		result = []
		path_copy = Field(self._engine, path.name)
		for i in range(start, end):
			path_copy.name[-1] = i
			result.append(Field(self._engine, path_copy.name, None))

		return result

	def _modifyFields(self, id, path, fields, remove_conflicts):
		"""Store values of given fields"""

		for field in fields:
			field.name = path.name + field.name

		if self._structure.objectHasField(id, path):
		# path already exists, delete it and all its children
			self._structure.deleteFields(id, [path])
		else:
		# path does not exist and is not root

			# check for list/map conflicts
			hierarchy = self._checkForConflicts(id, path, remove_conflicts)
			fields += hierarchy

			# fill autocreated list elements (if any) with Nones

			ancestors = path.getAncestors()
			del ancestors[0] # remove root object

			for ancestor in ancestors:
				if ancestor.pointsToListElement():
					fields += self._fillWithNones(id, ancestor)

		self._setFieldValues(id, fields)

	def processCreateRequest(self, request):
		new_id = self._engine.getNewId()
		self._modifyFields(new_id, interface.Field(self._engine, []), request.fields, True)
		return new_id

	def processModifyRequest(self, request):
		self._modifyFields(request.id, request.path,
			request.fields, request.remove_conflicts)

	def processDeleteRequest(self, request):

		if request.fields is not None:
			# remove specified fields
			self._structure.deleteFields(request.id, request.fields)

			# deletion of list element requires renumbering of other elements
			for field in request.fields:
				if len(field.name) > 0 and field.pointsToListElement():
					self._structure.renumberLists(request.id, field, -1)
		else:
			# delete whole object
			self._structure.deleteFields(request.id)

	def processReadRequest(self, request):

		path = None if (request.path is None or len(request.path.name) == 0) else request.path
		masks = None if (request.masks is None or len(request.masks) == 0) else request.masks

		# construct list of masks for reading
		if masks is None:
			if path is None:
				fields = None
			else:
				fields = [request.path]
		else:
			fields = []
			for mask in masks:
				if path is None or mask.matches(path):
					fields.append(mask)

		# get list of typed fields to read (whose tables are guaranteed to exist)
		fields_list = self._structure.getFlatFieldsInfo(request.id, fields)

		# read values
		result_list = self._structure.getFieldValues(request.id, fields_list)

		# if no fields were read - throw error (so that user could distinguish
		# this case from the case when None was read, for example)
		if len(result_list) == 0:

			if masks is None:
				if path is None:
					path_str = "exist"
				else:
					path_str = "have field " + str(request.path.name)

				raise interface.LogicError("Object " + str(request.id) +
					" does not " + path_str)
			else:
				raise interface.LogicError("Object " + str(request.id) +
					" does not have fields matching given masks")

		# remove root path from values
		if request.path is not None:
			for field in result_list:
				del field.name[:len(request.path.name)]

		return result_list

	def processSearchRequest(self, request):
		"""Search for all objects using given search condition"""

		def getMentionedFields(condition):
			if isinstance(condition.operand1, interface.SearchRequest.Condition):
				fields = getMentionedFields(condition.operand1)
				return fields.union(getMentionedFields(condition.operand2))
			else:
				return {condition.operand1.table_name}

		def updateCondition(condition, existing_tables):
			if isinstance(condition.operand1, interface.SearchRequest.Condition):
				updateCondition(condition.operand1, existing_tables)
				updateCondition(condition.operand2, existing_tables)
			else:
				if condition.operand1.table_name not in existing_tables:
					condition.operand1 = None

		if request.condition is not None:
			table_names = getMentionedFields(request.condition)
			table_names = self._engine.selectExistingTables(table_names)
			updateCondition(request.condition, table_names)

		request, tables, values = self._structure.buildSqlQuery(request.condition)
		if request is None:
			return []

		result = self._engine.execute(request, tables, values)
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

		# check that dictionary does not already exist at the place
		# where request.path is pointing to
		parent_field = Field(self._engine, request.path.name[:-1])
		if self._structure.objectHasField(request.id, parent_field):
			parent = self._structure.getFieldValues(request.id, [parent_field])
		else:
			parent = []

		if len(parent) == 0 or parent[0].py_value != list():
			if len(parent) == 0 or request.remove_conflicts:
			# try to autocreate list
				new_val = Field(self._engine, [], list())
				self._modifyFields(request.id, parent_field,
					[new_val], remove_conflicts=request.remove_conflicts)
			else:
			# in this case we can raise more meaningful error
				raise interface.StructureError("Cannot insert to non-list")

		# if path does not point to beginning of the list, fill
		# missing elements with Nones
		fields = []
		if request.path.name[-1] is not None and request.path.name[-1] > 0:
			fields += self._fillWithNones(request.id, request.path)
			max = request.path.name[-1]
		else:
			max = self._structure.getMaxListIndex(request.id, request.path)

		target_col = len(request.path.name) - 1 # last column in name of target field

		if max is None:
		# list does not exist yet
			enumerate(request.field_groups, target_col, 0)
		elif request.path.name[target_col] is None:
		# list exists and we are inserting elements to the end
			starting_num = max + 1
			enumerate(request.field_groups, target_col, starting_num)
		else:
		# list exists and we are inserting elements to the beginning or to the middle
			self._structure.renumberLists(request.id, request.path, len(request.field_groups))
			enumerate(request.field_groups, target_col, request.path.name[target_col])

		fields += functools.reduce(list.__add__, request.field_groups, [])
		self._setFieldValues(request.id, fields)

	def processObjectExistsRequest(self, request):
		return self._structure.objectExists(request.id)

	def processDumpRequest(self, request):
		ids = self.processSearchRequest(interface.SearchRequest())

		result = []
		for obj_id in ids:
			result += [obj_id, self.processReadRequest(interface.ReadRequest(obj_id))]

		return result

	def processRepairRequest(self, request):
		self._structure.repairSupportTables()
