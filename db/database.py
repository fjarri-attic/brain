import sqlite3
import re
import copy

from . import interface
from . import engine

class InternalField:

	def __init__(self, engine, name, value=None):
		if not isinstance(engine, interface.Engine):
			raise Exception("engine should be derived from Engine class")

		self.__engine = engine

		self.name = name[:]
		self.value = value
		self.type = 'text'

	@classmethod
	def fromNameStr(cls, engine, name_str, value=None):
		res = cls(engine, [], value)
		res.name_str = name_str
		return res

	def __get_safe_value(self):
		return self.__engine.getSafeValue(self.value)

	safe_value = property(__get_safe_value)

	def __get_name_str(self):
		return self.__engine.getNameString(['field'] + self.name)

	def __set_name_str(self, val):
		self.name = self.__engine.getNameList(val)[1:]

	name_str = property(__get_name_str, __set_name_str)

	def __get_name_as_table(self):
		return self.__engine.getSafeName(self.name_str)

	name_as_table = property(__get_name_as_table)

	def __get_name_as_value(self):
		return self.__engine.getSafeValue(self.name_str)

	name_as_value = property(__get_name_as_value)

	def __get_clean_name(self):
		return [(x if isinstance(x, str) else None) for x in self.name]

	def __get_columns_query(self):
		numeric_columns = filter(lambda x: not isinstance(x, str), self.name)
		counter = 0
		l = []
		for column in numeric_columns:
			if column == None:
				l.append("c" + str(counter))
			counter += 1

		return (', '.join([''] + l) if len(l) > 0 else '')

	def __get_columns_condition(self):
		numeric_columns = filter(lambda x: not isinstance(x, str), self.name)
		counter = 0
		l = []
		for column in numeric_columns:
			if column != None:
				l.append("c" + str(counter) + "=" + str(column))
			counter += 1

		return (' AND '.join([''] + l) if len(l) > 0 else '')

	def __get_undefined_positions(self):
		counter = 0
		l = []
		for elem in self.name:
			if elem == None:
				l.append(counter)
			counter += 1

		return l

	def __get_creation_str(self):
		counter = 0
		res = ""
		for elem in self.name:
			if not isinstance(elem, str):
				res += ", c" + str(counter) + " INTEGER"
				counter += 1

		return res

	def __get_columns_values(self):
		res = ""
		for elem in self.name:
			if not isinstance(elem, str):
				res += ", " + str(elem)

		return res

	clean_name = property(__get_clean_name)
	columns_query = property(__get_columns_query)
	columns_condition = property(__get_columns_condition)
	undefined_positions = property(__get_undefined_positions)
	creation_str = property(__get_creation_str)
	columns_values = property(__get_columns_values)

	def __str__(self):
		return "IField ('" + str(self.name) + "'" + \
			(", type=" + str(self.type) if self.type else "") + \
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
		self.__createSpecificationTable()

	#
	# Specification-oriented functions
	#

	def __createSpecificationTable(self):
		"""Create table (id, field) for storing information about objects' field names"""
		self.engine.execute("CREATE table IF NOT EXISTS {id_table} ({id_column} TEXT, {field_column} TEXT)"
			.format(id_table=self.__id_table, id_column=self.__ID_COLUMN,
			field_column=self.__FIELD_COLUMN))

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
			f = InternalField(self.engine, field.name, elem[0])

			# If field is a mask, there are list indexes in query result
			# We should fill field's Nones with them
			# FIXME: hide this in InternalField
			counter = 1
			for col in field.undefined_positions:
				f.name[col] = elem[counter]
				counter += 1

			res.append(f)

		if len(res) > 0:
			return res
		else:
			return None

	def __setFieldValue(self, id, field):
		"""Set value of given field"""

		# Create field table if it does not exist yet
		self.__assureFieldTableExists(field)

		# FIXME: check if UPDATE works
		# Delete old value
		self.engine.execute("DELETE FROM {field_name} WHERE id={id} {delete_condition}"
			.format(field_name=field.name_as_table, id=id, delete_condition=field.columns_condition))

		# Insert new value
		self.engine.execute("INSERT INTO {field_name} VALUES ({id}, '{type}', {value}{columns_values})"
			.format(field_name=field.name_as_table, id=id, type=field.type,
			value=field.safe_value, columns_values=field.columns_values))

	def deleteField(self, id, field):
		"""Delete given field(s)"""

		# check if table exists
		if not self.engine.tableExists(field.name_str):
			return

		# Check if we are:
		# 1) deleting fields from list
		# 2) not deleting the whole leaf list
		# FIXME: hide this in InternalField
		field_cols = list(filter(lambda x: not isinstance(x, str), field.name))
		if len(field_cols) > 0 and field_cols[-1] != None:
			# if we deleted something from list, we should re-enumerate list elements
			self.reenumerate(id, field, -1)
		else:
			# FIXME: these actions are pretty identical to what is done in reenumerate()
			# delete value
			self.engine.execute("DELETE FROM {field_name} WHERE id={id}{delete_condition}"
				.format(field_name=field.name_as_table, id=id, delete_condition=field.columns_condition))

			# check if the table is empty and if it is - delete it too
			if self.engine.tableIsEmpty(field.name_str):
				self.engine.deleteTable(field.name_str)

	def __assureFieldTableExists(self, field):
		"""Create table for storing values of this field if it does not exist yet"""

		# Compose columns list
		# FIXME: hide this in InternalField
		values_str = "id TEXT, type TEXT, value TEXT" + field.creation_str

		# Create table
		self.engine.execute("CREATE TABLE IF NOT EXISTS {field_name} ({values_str})"
			.format(field_name=field.name_as_table, values_str=values_str))

	def createObject(self, id, fields):
		"""Create new object with given fields"""

		# create object header
		for field in fields:
			self.__updateSpecification(id, field)

		# update field tables
		for field in fields:
			self.__assureFieldTableExists(field)
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

		# we assume here that all columns in field are defined except for the last one
		
		# FIXME: hide this into InternalField
		query = field.columns_query[2:] # removing first ','
		
		l = self.engine.execute("SELECT MAX ({query}) FROM {field_name} WHERE id={id}{columns_condition}"
			.format(query=query, field_name=field.name_as_table, id=id,
			columns_condition=field.columns_condition))

		res = l[0][0]
		return res

	def reenumerate(self, id, target_field, shift):
		"""Reenumerate list elements before insertion or deletion"""

		# Get the name and the value of last numerical column
		field_cols = list(filter(lambda x: not isinstance(x, str), target_field.name))
		col_num = len(field_cols) - 1
		col_name = "c" + str(col_num)
		col_val = field_cols[col_num]

		# Get all child field names
		fields_to_reenum = self.getFieldsList(id, target_field)
		for fld in fields_to_reenum:

			# if shift is negative, we should delete elements first
			if shift < 0:
				self.engine.execute("DELETE FROM {field_name} WHERE id={id} AND {col_name}={col_val}"
					.format(field_name=fld.name_as_table, id=id, col_name=col_name, col_val=col_val))

				if self.engine.tableIsEmpty(fld.name_str):
					self.engine.deleteTable(fld.name_str)

			# shift numbers of all elements in list 
			self.engine.execute("UPDATE {field_name} SET {col_name}={col_name}+{shift} WHERE id={id} AND {col_name}>={col_val}"
				.format(field_name=fld.name_as_table, col_name=col_name, shift=shift, id=id, col_val=col_val))

class SimpleDatabase(interface.Database):
	"""Class, representing OODB over SQL"""

	def __init__(self, path, engine_class):
		if not issubclass(engine_class, interface.Engine):
			raise Exception("Engine class must be derived from Engine interface")
		self.engine = engine_class(path)
		self.structure = StructureLayer(self.engine)

	def processRequest(self, request):
		"""Process given request and return results"""

		def convertFields(fields, engine):
			if fields != None:
				return [InternalField(engine, x.name, x.value) for x in fields]
			else:
				return None

		def convertCondition(condition, engine):
			if condition.leaf:
				condition.operand1 = InternalField(engine,
					condition.operand1.name, condition.operand1.value)
			else:
				convertCondition(condition.operand1, engine)
				convertCondition(condition.operand2, engine)

		if isinstance(request, interface.ModifyRequest):
			self.__processModifyRequest(
				request.id,
				convertFields(request.fields, self.engine))

		elif isinstance(request, interface.DeleteRequest):
			self.__processDeleteRequest(
				request.id,
				convertFields(request.fields, self.engine))

		elif isinstance(request, interface.SearchRequest):
			condition_copy = copy.deepcopy(request.condition)
			convertCondition(condition_copy, self.engine)
			return self.__processSearchRequest(condition_copy)

		elif isinstance(request, interface.ReadRequest):
			return self.__processReadRequest(
				request.id,
				convertFields(request.fields, self.engine))

		elif isinstance(request, interface.InsertRequest):
			self.__processInsertRequest(
				request.id,
				InternalField(self.engine, request.target_field.name, request.target_field.value),
				convertFields(request.fields, self.engine),
				request.one_position)
		else:
			raise Exception("Unknown request type: " + request.__class__.__name__)

	def __processInsertRequest(self, id, target_field, fields, one_position):

		def enumerate(fields_list, col_num, starting_num, one_position=False):
			counter = starting_num
			for field in fields_list:
				field.name[col_num] = counter
				if not one_position:
					counter += 1

		if not self.structure.objectExists(id):
			raise Exception("Object " + id + " does not exist")

		target_col = len(target_field.name) - 1 # last column in name of target field

		if not self.structure.objectHasField(id, target_field):
			enumerate(fields, target_col, 0, one_position)
		elif target_field.name[target_col] == None:
			starting_num = self.structure.getMaxNumber(id, target_field) + 1
			enumerate(fields, target_col, starting_num, one_position)
		else:
			self.structure.reenumerate(id, target_field,
				(1 if one_position else len(fields)))
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
		if fields:
			fields_to_read = filter(lambda x: self.structure.objectHasField(id, x), fields)
		else:
			fields_to_read = self.structure.getFieldsList(id)

		results = [self.structure.getFieldValue(id, field) for field in fields_to_read]

		result_list = []
		for result in results:
			if result != None:
				result_list += result
		return [interface.Field(x.name, x.value) for x in result_list]

	def __processSearchRequest(self, condition):

		def propagateInversion(condition):
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
