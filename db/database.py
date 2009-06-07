import sqlite3
import re
import copy

from . import interface
from . import engine

class StructureError(Exception):
	"""Request tried to do something conflicting with DB structure"""
	pass

class InternalField:
	"""Class for more convenient handling of Field objects"""

	def __init__(self, engine, name, value=None):
		self.__engine = engine
		self.name = name[:]
		self.value = value

	def __getListColumnName(self, index):
		"""Get name of additional list column corresponding to given index"""
		return "c" + str(index)

	@classmethod
	def fromNameStr(cls, engine, name_str, value=None):
		"""Create object using stringified name instead of list"""
		return cls(engine, engine.getNameList(name_str)[1:], value)

	@property
	def safe_value(self):
		"""Returns value in form that can be safely used as value in queries"""
		return self.__engine.getSafeValue(self.value)

	@property
	def name_str(self):
		"""Returns field name in string form"""
		return self.__engine.getNameString(['field'] + self.name)

	@property
	def name_as_table(self):
		"""Returns field name in form that can be safely used as a table name"""
		return self.__engine.getSafeName(self.name_str)

	@property
	def name_as_value(self):
		"""Returns field name in form that can be safely used as value in queries"""
		return self.__engine.getSafeValue(self.name_str)

	@property
	def clean_name(self):
		"""Returns name with only hash elements"""
		return [(x if isinstance(x, str) else None) for x in self.name]

	@property
	def columns_query(self):
		"""Returns string with additional values list necessary to query the value of this field"""
		numeric_columns = filter(lambda x: not isinstance(x, str), self.name)
		counter = 0
		l = []
		for column in numeric_columns:
			if column == None:
				l.append(self.__getListColumnName(counter))
			counter += 1

		return (', '.join([''] + l) if len(l) > 0 else '')

	@property
	def columns_condition(self):
		"""Returns string with condition for operations on given field"""

		# do not skip Nones, because we need them for
		# getting proper index of list column
		numeric_columns = filter(lambda x: not isinstance(x, str), self.name)
		counter = 0
		l = []
		for column in numeric_columns:
			if column != None:
				l.append(self.__getListColumnName(counter) +
					"=" + str(column))
			counter += 1

		return (' AND '.join([''] + l) if len(l) > 0 else '')

	def getDeterminedName(self, vals):
		"""Returns name with Nones filled with supplied list of values"""
		vals_copy = list(vals)
		func = lambda x: vals_copy.pop(0) if x == None else x
		return list(map(func, self.name))

	@property
	def creation_str(self):
		"""Returns string containing list of columns necessary to create field table"""
		counter = 0
		res = ""
		for elem in self.name:
			if not isinstance(elem, str):
				res += ", " + self.__getListColumnName(counter) + " INTEGER"
				counter += 1

		return "id TEXT, value TEXT" + res

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
		list_elems = self.__getListElements()
		return len(list_elems) > 0 and list_elems[-1] != None

	def pointsToList(self):
		"""Returns True if last name element corresponds to list"""
		return not isinstance(self.name[-1], str)

	def getLastListColumn(self):
		"""Returns name and value of column corresponding to the last name element"""

		# This function makes sense only if self.pointsToListElement() is True
		if not self.pointsToListElement():
			return None, None

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
			return None, None

		self_copy = InternalField(self.__engine, self.name)
		self_copy.name[-1] = None
		return self_copy.columns_condition

	@property
	def name_hashstr(self):
		"""
		Returns string that can serve as hash for field name along with its list elements
		FIXME: currently there are possible collisions name = ['1'] and name = [1]
		"""
		name_copy = [str(x) if x != None else None for x in self.name]
		name_copy[-1] = None
		return self.__engine.getSafeValue(self.__engine.getNameString(name_copy))

	def __str__(self):
		return "IField ('" + str(self.name) + "'" + \
			(", value=" + str(self.value) if self.value else "") + ")"

	def __repr__(self):
		return str(self)

class StructureLayer:
	"""Class which is connected to DB engine and incapsulates all SQL queries"""

	__ID_TABLE = 'id' # name of table with object specifications
	__ID_COLUMN = 'id' # name of column with object id in all tables
	__FIELD_COLUMN = 'field' # name of column with field names in specification table

	def __init__(self, engine):
		self.engine = engine

		# memorize string with specification table name
		self.__id_table = self.engine.getSafeName(
			self.engine.getNameString([self.__ID_TABLE]))

		# create specification table
		self.engine.begin()
		self.__createSpecificationTable()
		self.engine.commit()

	#
	# Specification-oriented functions
	#

	def __createSpecificationTable(self):
		"""Create table (id, field) for storing information about objects' field names"""
		self.engine.execute("CREATE table IF NOT EXISTS {id_table} ({id_column} TEXT, {field_column} TEXT)"
			.format(id_table=self.__id_table, id_column=self.__ID_COLUMN,
			field_column=self.__FIELD_COLUMN))

		self.engine.execute("CREATE table IF NOT EXISTS listsizes (id TEXT, field TEXT, max INTEGER)")

	def __deleteSpecification(self, id):
		"""Delete all information about object from specification table"""
		self.engine.execute("DELETE FROM {id_table} WHERE {id_column}={id}"
			.format(id_table=self.__id_table, id_column=self.__ID_COLUMN, id=id))

	def __updateSpecification(self, id, field):
		"""If information about given field does not exist in specification table, add it"""

		# Check if field exists in specification
		l = self.engine.execute("SELECT field FROM {id_table} WHERE {id_column}={id} AND {field_column}={field_name}"
			.format(id_table=self.__id_table, id_column=self.__ID_COLUMN, id=id,
			field_column=self.__FIELD_COLUMN, field_name=field.name_as_value))

		if len(l) == 0:
			# Add field to specification
			self.engine.execute("INSERT INTO {id_table} VALUES ({id}, {field_name})"
				.format(id_table=self.__id_table, id=id, field_name=field.name_as_value))

	def getFieldsList(self, id, field=None):
		"""Get list of fields for given object"""

		# If field is given, return only fields, which contain its name in the beginning
		regexp_cond = ((" AND {field_column} REGEXP {regexp}") if field != None else "")
		regexp_val = (self.engine.getSafeValue("^" + field.name_str) if field != None else None)

		# Get list of fields
		l = self.engine.execute(("SELECT {field_column} FROM {id_table} WHERE {id_column}={id}" + regexp_cond)
			.format(id_table=self.__id_table, id=id, regexp=regexp_val,
			id_column=self.__ID_COLUMN, field_column=self.__FIELD_COLUMN))

		return [InternalField.fromNameStr(self.engine, x[0]) for x in l]

	def objectExists(self, id):
		"""Check if object exists in database"""

		# We need just check if there is at least one row with its id
		# in specification table
		l = self.engine.execute("SELECT {field_column} FROM {id_table} WHERE {id_column}={id}"
			.format(id_table=self.__id_table, id=id,
			id_column=self.__ID_COLUMN, field_column=self.__FIELD_COLUMN))
		return len(l) > 0

	#
	# Other functions
	#

	def getFieldValue(self, id, field):
		"""Read value of given field(s)"""

		# Get field values
		# If field is a mask (i.e., contains Nones), there will be more than one result
		l = self.engine.execute("SELECT value{columns_query} FROM {field_name} WHERE id={id}{columns_condition}"
			.format(columns_query=field.columns_query, field_name=field.name_as_table,
			id=id, columns_condition=field.columns_condition))

		# Convert results to list of InternalFields
		res = []
		for elem in l:
			res.append(InternalField(self.engine, field.getDeterminedName(elem[1:]), elem[0]))

		if len(res) > 0:
			return res
		else:
			return None

	def __updateListSize(self, id, field, val):
		max = self.getMaxNumber(id, field)
		if max != None:
			if max > val:
				return

			self.engine.execute("DELETE FROM listsizes WHERE id={id} AND field={field_name}"
				.format(id=id, field_name=field.name_hashstr))

		self.engine.execute("INSERT INTO listsizes VALUES ({id}, {field_name}, {val})"
			.format(id=id, field_name=field.name_hashstr, val=val))

	def __setFieldValue(self, id, field):
		"""Set value of given field"""

		# Update maximum values cache
		# FIXME: hide .name usage in InternalField
		name_copy = field.name[:]
		while len(name_copy) > 0:
			if isinstance(name_copy[-1], int):
				f = InternalField(self.engine, name_copy)
				self.__updateListSize(id, f, name_copy[-1])
			name_copy.pop()

		# Create field table if it does not exist yet
		self.__assureFieldTableExists(field)

		# Delete old value
		self.engine.execute("DELETE FROM {field_name} WHERE id={id} {delete_condition}"
			.format(field_name=field.name_as_table, id=id, delete_condition=field.columns_condition))

		# Insert new value
		self.engine.execute("INSERT INTO {field_name} VALUES ({id}, {value}{columns_values})"
			.format(field_name=field.name_as_table, id=id,
			value=field.safe_value, columns_values=field.columns_values))

	def deleteValues(self, id, field, condition=None):
		"""Delete value of given field(s)"""
		if condition == None:
			condition = field.columns_condition

		# delete value
		self.engine.execute("DELETE FROM {field_name} WHERE id={id}{delete_condition}"
			.format(field_name=field.name_as_table, id=id, delete_condition=condition))

		# check if the table is empty and if it is - delete it too
		if self.engine.tableIsEmpty(field.name_str):
			self.engine.deleteTable(field.name_str)
			return True
		else:
			return False

	def deleteField(self, id, field):
		"""Delete given field(s)"""

		# we can avoid unnecessary work by checking if table exists
		if not field.pointsToList() and not self.engine.tableExists(field.name_str):
			return

		if field.pointsToListElement():
			# deletion of list element requires renumbering of other elements
			self.renumber(id, field, -1)
		else:
			# otherwise just delete values using given field mask
			self.deleteValues(id, field)

	def __assureFieldTableExists(self, field):
		"""Create table for storing values of this field if it does not exist yet"""

		# Create table
		self.engine.execute("CREATE TABLE IF NOT EXISTS {field_name} ({values_str})"
			.format(field_name=field.name_as_table, values_str=field.creation_str))

	def createObject(self, id, fields):
		"""Create new object with given fields"""

		for field in fields:
			self.__updateSpecification(id, field) # create object header
			self.__assureFieldTableExists(field) # create field table
			self.__setFieldValue(id, field)

	def deleteObject(self, id):
		"""Delete object with given ID"""

		fields = self.getFieldsList(id)

		# for each field, remove it from tables
		for field in fields:
			self.deleteField(id, field)

		self.__deleteSpecification(id)

	def objectHasField(self, id, field):
		"""Check if object has some field"""
		existing_fields = self.getFieldsList(id)

		# FIXME: hide .name usage in InternalField
		existing_names = [existing_field.name for existing_field in existing_fields]
		return field.clean_name in existing_names

	def modifyObject(self, id, fields):
		"""Update object using given list of fields"""

		# for each field, check if it already exists and update specification if necessary
		for field in fields:
			if not self.objectHasField(id, field):
				self.__updateSpecification(id, field)

			self.__setFieldValue(id, field)

	def searchForObjects(self, condition):
		"""Search for all objects using given search condition"""

		def buildSqlQuery(condition):
			"""Recursive function to transform condition into SQL query"""

			if not condition.leaf:
				# child conditions
				cond1 = buildSqlQuery(condition.operand1)
				cond2 = buildSqlQuery(condition.operand2)

				# 'And' corresponds to the intersection of sets
				if isinstance(condition.operator, interface.SearchRequest.And):
					return ("SELECT * FROM ({cond1}) INTERSECT SELECT * FROM ({cond2})"
						.format(cond1=cond1, cond2=cond2))
				# 'Or' corresponds to the union of sets
				elif isinstance(condition.operator, interface.SearchRequest.Or):
					return ("SELECT * FROM ({cond1}) UNION SELECT * FROM ({cond2})"
						.format(cond1=cond1, cond2=cond2))
				else:
					raise Exception("Operator unsupported: " + str(condition.operator))
				return

			# Leaf condition
			op1 = condition.operand1 # it must be Field
			op2 = condition.operand2 # it must be some value

			safe_name = condition.operand1.name_as_table

			# If table with given field does not exist, just return empty query
			if not self.engine.tableExists(op1.name_str):
				return self.engine.getEmptyCondition()

			not_str = " NOT " if condition.invert else " "
			op2_val = self.engine.getSafeValue(op2)

			if isinstance(condition.operator, interface.SearchRequest.Eq):
				result = "SELECT DISTINCT id FROM {field_name} WHERE{not_str}value={val}{columns_condition}"\
					.format(field_name=safe_name, not_str=not_str,
					val=op2_val, columns_condition=op1.columns_condition)
			elif isinstance(condition.operator, interface.SearchRequest.Regexp):
				result = "SELECT DISTINCT id FROM {field_name} WHERE{not_str}value REGEXP {val}{columns_condition}"\
					.format(field_name=safe_name, not_str=not_str,
					val=op2_val, columns_condition=op1.columns_condition)
			else:
				raise Exception("Comparison unsupported: " + str(condition.operator))

			if condition.invert:
				result += " UNION SELECT * FROM (SELECT id FROM {id_table} EXCEPT SELECT id FROM {field_name})"\
					.format(id_table=self.__id_table, field_name=safe_name)

			return result

		request = buildSqlQuery(condition)
		result = self.engine.execute(request)
		list_res = [x[0] for x in result]

		return list_res

	def getMaxNumber(self, id, field):
		"""Get maximum value of list index for the undefined column of the field"""

		l = self.engine.execute("SELECT max FROM listsizes WHERE id={id} AND field={field_name}"
			.format(id=id, field_name=field.name_hashstr))

		if len(l) > 0:
			return l[0][0]
		else:
			return None

	def renumber(self, id, target_field, shift):
		"""Renumber list elements before insertion or deletion"""

		# Get the name and the value of last numerical column
		col_name, col_val = target_field.getLastListColumn()
		cond = target_field.renumber_condition

		# Get all child field names
		fields_to_reenum = self.getFieldsList(id, target_field)
		for fld in fields_to_reenum:

			# if shift is negative, we should delete elements first
			table_was_deleted = False
			if shift < 0:
				table_was_deleted = self.deleteValues(id, fld, target_field.columns_condition)

			# shift numbers of all elements in list
			if not table_was_deleted:
				self.engine.execute("UPDATE {field_name} SET {col_name}={col_name}+{shift} WHERE id={id}{cond} AND {col_name}>={col_val}"
					.format(field_name=fld.name_as_table, col_name=col_name,
					shift=shift, id=id, col_val=col_val, cond=cond))

class SimpleDatabase(interface.Database):
	"""Class, representing OODB over SQL"""

	def __init__(self, path, engine_class):
		if not issubclass(engine_class, interface.Engine):
			raise Exception("Engine class must be derived from Engine interface")
		self.engine = engine_class(path)
		self.structure = StructureLayer(self.engine)

	def processRequest(self, request):
		"""Start/stop transaction, handle exceptions"""
		self.engine.begin()
		try:
			res = self.__processRequest(request)
		except:
			self.engine.rollback()
			raise
		self.engine.commit()
		return res

	def __processRequest(self, request):
		"""Process given request and return results"""

		def convertFields(fields, engine):
			"""Convert given fields list to InternalFields list"""
			if fields != None:
				return [InternalField(engine, x.name, x.value) for x in fields]
			else:
				return None

		def convertCondition(condition, engine):
			"""Convert fields in given condition to InternalFields"""
			if condition.leaf:
				condition.operand1 = InternalField(engine,
					condition.operand1.name, condition.operand1.value)
			else:
				convertCondition(condition.operand1, engine)
				convertCondition(condition.operand2, engine)

		# ModifyRequest
		if isinstance(request, interface.ModifyRequest):
			self.__processModifyRequest(
				request.id,
				convertFields(request.fields, self.engine))

		# DeleteRequest
		elif isinstance(request, interface.DeleteRequest):
			self.__processDeleteRequest(
				request.id,
				convertFields(request.fields, self.engine))

		# SearchRequest
		elif isinstance(request, interface.SearchRequest):
			condition_copy = copy.deepcopy(request.condition)
			convertCondition(condition_copy, self.engine)
			return self.__processSearchRequest(condition_copy)

		# ReadRequest
		elif isinstance(request, interface.ReadRequest):
			return self.__processReadRequest(
				request.id,
				convertFields(request.fields, self.engine))

		# InsertRequest
		elif isinstance(request, interface.InsertRequest):

			# fields to insert have relative names
			for field in request.fields:
				field.name = request.target_field.name + field.name

			self.__processInsertRequest(
				request.id,
				InternalField(self.engine, request.target_field.name, request.target_field.value),
				convertFields(request.fields, self.engine),
				request.one_position)
		else:
			raise Exception("Unknown request type: " + request.__class__.__name__)

	def __processInsertRequest(self, id, target_field, fields, one_position):

		def enumerate(fields_list, col_num, starting_num, one_position=False):
			"""Enumerate given column in list of fields"""
			counter = starting_num
			for field in fields_list:
				# FIXME: Hide .name usage in InternalField
				field.name[col_num] = counter
				if not one_position:
					counter += 1

		if not self.structure.objectExists(id):
			raise Exception("Object " + id + " does not exist")

		# FIXME: Hide .name usage in InternalField
		target_col = len(target_field.name) - 1 # last column in name of target field

		max = self.structure.getMaxNumber(id, target_field)
		if max == None:
		# list does not exist yet
			enumerate(fields, target_col, 0, one_position)
		# FIXME: Hide .name usage in InternalField
		elif target_field.name[target_col] == None:
		# list exists and we are inserting elements to the end
			starting_num = max + 1
			enumerate(fields, target_col, starting_num, one_position)
		else:
		# list exists and we are inserting elements to the beginning or to the middle
			self.structure.renumber(id, target_field,
				(1 if one_position else len(fields)))
			# FIXME: Hide .name usage in InternalField
			enumerate(fields, target_col, target_field.name[target_col], one_position)

		self.__processModifyRequest(id, fields)

	def __processModifyRequest(self, id, fields):

		# check if the entry with specified id already exists
		# if no, just add it to the database
		if not self.structure.objectExists(id):
			self.structure.createObject(id, fields)
		else:
			self.structure.modifyObject(id, fields)

	def __processDeleteRequest(self, id, fields):

		if fields != None:
			# remove specified fields
			for field in fields:
				self.structure.deleteField(id, field)
			return
		else:
			# delete whole object
			self.structure.deleteObject(id)

	def __processReadRequest(self, id, fields):

		# if list of fields was not given, read all object's fields
		if fields == None:
			fields = self.structure.getFieldsList(id)

		# Fixme: maybe it is worth making a separate function in Structure layer
		results = [self.structure.getFieldValue(id, field) for field in fields]

		result_list = []
		for result in results:
			if result != None:
				result_list += result
		# FIXME: Hide .name usage in InternalField
		return [interface.Field(x.name, x.value) for x in result_list]

	def __processSearchRequest(self, condition):

		def propagateInversion(condition):
			"""Propagate inversion flags to the leafs of condition tree"""

			if not condition.leaf:
				if condition.invert:

					condition.invert = False

					condition.operand1.invert = not condition.operand1.invert
					condition.operand2.invert = not condition.operand2.invert

					if isinstance(condition.operator, interface.SearchRequest.And):
						condition.operator = interface.SearchRequest.Or()
					elif isinstance(condition.operator, interface.SearchRequest.Or):
						condition.operator = interface.SearchRequest.And()

				propagateInversion(condition.operand1)
				propagateInversion(condition.operand2)

		propagateInversion(condition)
		return self.structure.searchForObjects(condition)
