import sqlite3
import re
import copy

from . import interface
from . import engine

class InternalField:

	def __init__(self, field, engine):
		if not isinstance(field, interface.Field):
			raise Exception("field should be an instance of Field class")
		if not isinstance(engine, interface.Engine):
			raise Exception("engine should be derived from Engine class")

		self.__field = field
		self.__engine = engine

		self.name = field.name
		self.value = field.value
		self.type = field.type

	def __get_safe_value(self):
		return self.__engine.getSafeValue(self.__field.value)

	def __set_safe_value(self, val):
		self.__field.value = self.__engine.getUnsafeValue(val)

	def __get_safe_name(self):
		return self.__engine.getSafeTableName(['field'] + self.__field.name)

	def __set_safe_name(self, val):
		self.__field.name = (self.__engine.getFieldName(val))[1:]

	def __get_name_as_safe_value(self):
		return "'" + self.safe_name + "'"

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

	safe_value = property(__get_safe_value, __set_safe_value)
	safe_name = property(__get_safe_name, __set_safe_name)
	name_as_safe_value = property(__get_name_as_safe_value)
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

	__ID_TABLE = 'id' # name of table with object specifications
	__ID_COLUMN = 'id' # name of column with object id in all tables
	__FIELD_COLUMN = 'field' # name of column with field names in specification table

	def __init__(self, engine):
		self.engine = engine
		self.__id_table = engine.getSafeTableName([self.__ID_TABLE])
		self.__createSpecificationTable()

	#
	# Specification-oriented functions
	#

	def __createSpecificationTable(self):
		self.engine.execute("CREATE table IF NOT EXISTS {id_table} ({id_column} TEXT, {field_column} TEXT)"
			.format(id_table=self.__id_table, id_column=self.__ID_COLUMN,
			field_column=self.__FIELD_COLUMN))

	def __createSpecification(self, id, fields):
		for field in fields:
			self.__updateSpecification(id, field)

	def __deleteSpecification(self, id):
		self.engine.execute("DELETE FROM {id_table} WHERE id={id}"
			.format(id_table=self.__id_table, id=id))

	def __updateSpecification(self, id, field):

		l = self.engine.execute("SELECT field FROM {id_table} WHERE id={id} AND field={field_name}"
			.format(id_table=self.__id_table, id=id, field_name=field.name_as_safe_value))

		if len(l) == 0:
			self.engine.execute("INSERT INTO {id_table} VALUES ({id}, {field_name})"
				.format(id_table=self.__id_table, id=id, field_name=field.name_as_safe_value))

	def getFieldsList(self, id, field=None):
		# get object specification

		# FIXME: we should use ^{field_name} regexp
		regexp_cond = ((" AND field REGEXP '{regexp}'") if field != None else "")

		# FIXME: we should not depend on "safe table name" format here
		regexp_val = (field.safe_name[1:-1] if field != None else None)

		l = self.engine.execute(("SELECT field FROM {id_table} WHERE id={id}" + regexp_cond)
			.format(id_table=self.__id_table, id=id, regexp=regexp_val))

		field_names = [self.engine.getFieldName(x[0])[1:] for x in l]

		return [InternalField(interface.Field(x), self.engine) for x in field_names]

	def objectExists(self, id):
		l = self.engine.execute("SELECT field FROM {id_table} WHERE id={id}"
			.format(id_table=self.__id_table, id=id))
		return len(l) > 0

	#
	# Other functions
	#

	def getFieldValue(self, id, field):

		l = self.engine.execute("SELECT value{columns_query} FROM {field_name} WHERE id={id}{columns_condition}"
			.format(columns_query=field.columns_query, field_name=field.safe_name,
			id=id, columns_condition=field.columns_condition))

		res = []

		for elem in l:
			f = InternalField(interface.Field(field.name, elem[0]), self.engine)

			counter = 1
			for col in field.undefined_positions:
				f.name[col] = elem[counter]
				counter += 1

			res.append(f)

		if len(res) > 1:
			return res
		elif len(l) == 1:
			return res
		else:
			return None

	def __setFieldValue(self, id, field):
		self.__assureFieldTableExists(field)

		self.engine.execute("DELETE FROM {field_name} WHERE id={id} {delete_condition}"
			.format(field_name=field.safe_name, id=id, delete_condition=field.columns_condition))
		self.engine.execute("INSERT INTO {field_name} VALUES ({id}, '{type}', {value}{columns_values})"
			.format(field_name=field.safe_name, id=id, type=field.type,
			value=field.safe_value, columns_values=field.columns_values))

	def deleteField(self, id, field):

		# check if table exists
		if not self.engine.tableExists(field.safe_name):
			return

		# if we deleted something from list, we should re-enumerate list elements
		field_cols = list(filter(lambda x: not isinstance(x, str), field.name))
		if len(field_cols) > 0 and field_cols[-1] != None:
			self.reenumerate(id, field, -1)
		else:
			# delete value
			self.engine.execute("DELETE FROM {field_name} WHERE id={id}{delete_condition}"
				.format(field_name=field.safe_name, id=id, delete_condition=field.columns_condition))

			# check if the table is empty and if it is - delete it too
			if self.engine.tableIsEmpty(field.safe_name):
				self.engine.deleteTable(field.safe_name)

	def __assureFieldTableExists(self, field):
		values_str = "id TEXT, type TEXT, value TEXT" + field.creation_str

		self.engine.execute("CREATE TABLE IF NOT EXISTS {field_name} ({values_str})"
			.format(field_name=field.safe_name, values_str=values_str))

	def createObject(self, id, fields):

		# create object header
		self.__createSpecification(id, fields)

		# update field tables
		for field in fields:
			self.__assureFieldTableExists(field)
			self.__setFieldValue(id, field)

	def deleteObject(self, id):

		fields = self.getFieldsList(id)

		# for each field, remove it from tables
		for field in fields:
			self.deleteField(id, field)

		self.__deleteSpecification(id)

	def objectHasField(self, id, field):
		existing_fields = self.getFieldsList(id)
		existing_names = [existing_field.name for existing_field in existing_fields]
		return field.clean_name in existing_names

	def modifyObject(self, id, fields):

		# for each field, check if it already exists
		for field in fields:
			if self.objectHasField(id, field):
				self.__setFieldValue(id, field)
			else:
				self.__updateSpecification(id, field)
				self.__setFieldValue(id, field)

	def searchForObjects(self, condition):
		def buildSqlQuery(condition):

			if not condition.leaf:
				if isinstance(condition.operator, interface.SearchRequest.And):
					return ("SELECT * FROM ({cond1}) INTERSECT SELECT * FROM ({cond2})")\
						.format(cond1=buildSqlQuery(condition.operand1),\
						cond2=buildSqlQuery(condition.operand2))
				elif isinstance(condition.operator, interface.SearchRequest.Or):
					return ("SELECT * FROM ({cond1}) UNION SELECT * FROM ({cond2})")\
						.format(cond1=buildSqlQuery(condition.operand1),\
						cond2=buildSqlQuery(condition.operand2))
				else:
					raise Exception("Operator unsupported: " + str(condition.operator))
				return

			safe_name = condition.operand1.safe_name

			if not self.engine.tableExists(safe_name):
				return self.engine.getEmptyCondition()

			if condition.invert:
				not_str = " NOT "
			else:
				not_str = " "

			if isinstance(condition.operator, interface.SearchRequest.Eq):
				result = "SELECT DISTINCT id FROM {field_name} WHERE{not_str}value='{val}'{columns_condition}"\
					.format(field_name=safe_name, not_str=not_str,
					val=condition.operand2, columns_condition=condition.operand1.columns_condition)
			elif isinstance(condition.operator, interface.SearchRequest.Regexp):
				result = "SELECT DISTINCT id FROM {field_name} WHERE{not_str}value REGEXP '{val}'{columns_condition}"\
					.format(field_name=safe_name, not_str=not_str,
					val=condition.operand2, columns_condition=condition.operand1.columns_condition)
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

		# we assume here that all columns in field are defined except for the last one
		query = field.columns_query[2:] # removing first ','
		l = self.engine.execute("SELECT MAX ({query}) FROM {field_name} WHERE id={id}{columns_condition}"
			.format(query=query, field_name=field.safe_name, id=id,
			columns_condition=field.columns_condition))

		res = l[0][0]
		return res

	def reenumerate(self, id, target_field, shift):
		field_cols = list(filter(lambda x: not isinstance(x, str), target_field.name))
		col_num = len(field_cols) - 1
		col_name = "c" + str(col_num)
		col_val = field_cols[col_num]

		fields_to_reenum = self.getFieldsList(id, target_field)
		for fld in fields_to_reenum:

			# if shift is negative, we should delete elements first
			if shift < 0:
				self.engine.execute("DELETE FROM {field_name} WHERE id={id} AND {col_name}={col_val}"
					.format(field_name=fld.safe_name, id=id, col_name=col_name, col_val=col_val))

				if self.engine.tableIsEmpty(fld.safe_name):
					self.engine.deleteTable(fld.safe_name)

			self.engine.execute("UPDATE {field_name} SET {col_name}={col_name}+{shift} WHERE id={id} AND {col_name}>={col_val}"
				.format(field_name=fld.safe_name, col_name=col_name, shift=shift, id=id, col_val=col_val))

class SimpleDatabase(interface.Database):

	def __init__(self, path, engine_class):
		if not issubclass(engine_class, interface.Engine):
			raise Exception("Engine class must be derived from Engine interface")
		self.engine = engine_class(path)
		self.structure = StructureLayer(self.engine)

	def processRequest(self, request):

		def convertFields(fields, engine):
			if fields != None:
				return [InternalField(x, engine) for x in fields]
			else:
				return None

		def convertCondition(condition, engine):
			if condition.leaf:
				condition.operand1 = InternalField(condition.operand1, engine)
			else:
				convertCondition(condition.operand1, engine)
				convertCondition(condition.operand2, engine)

		if isinstance(request, interface.ModifyRequest):
			self.__processModifyRequest(
				self.engine.getSafeValue(request.id),
				convertFields(request.fields, self.engine))

		elif isinstance(request, interface.DeleteRequest):
			self.__processDeleteRequest(
				self.engine.getSafeValue(request.id),
				convertFields(request.fields, self.engine))

		elif isinstance(request, interface.SearchRequest):
			condition_copy = copy.deepcopy(request.condition)
			convertCondition(condition_copy, self.engine)
			return self.__processSearchRequest(condition_copy)

		elif isinstance(request, interface.ReadRequest):
			return self.__processReadRequest(
				self.engine.getSafeValue(request.id),
				convertFields(request.fields, self.engine))

		elif isinstance(request, interface.InsertRequest):
			self.__processInsertRequest(
				self.engine.getSafeValue(request.id),
				InternalField(request.target_field, self.engine),
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
