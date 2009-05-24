import sqlite3
import re

from . import interface
from . import engine

_ID_TABLE = 'id'
_ID_COLUMN = 'id'

class InternalField:

	def __init__(self, field, engine):
		if not isinstance(field, interfaces.Field):
			raise Exception("field should be an instance of Field class")
		if not isinstance(engine, interfaces.Engine):
			raise Exception("engine should be derived from Engine class")

		self.__field = field
		self.__engine = engine

	def __get_safe_value(self):
		return self.__engine.getSafeValueFromString(self.__field.value)

	def __set_safe_value(self, val):
		self.__field.value = self.__engine.getStringFromSafeValue(val)

	def __get_safe_name(self):
		return "field." + self.__engine.getSafeTableNameFromList(self.__field.name)

	def __set_safe_name(self, val):
		self.__field.name = (self.__engine.getListFromSafeTableName(val))[6:]

	safe_value = property(__get_safe_value, __set_safe_value)
	safe_name = property(__get_safe_name, __set_safe_name)

def _cleanColsFromList(name_list):
	return [(x if isinstance(x, str) else None) for x in name_list]

def _conditionFromList(name_list):
	num_cols = filter(lambda x: not isinstance(x, str), name_list)

	counter = 0
	cond_list = []
	cond_raw_list = []
	query_list = []
	query_cols = []
	param_map = {}
	for num_col in num_cols:
		if num_col != None:
			cond_list.append("c" + str(counter) + "=:c" + str(counter))
			param_map["c" + str(counter)] = num_col
			cond_raw_list.append("c" + str(counter) + "=" + str(num_col))
		else:
			query_list.append("c" + str(counter))
		counter += 1

	counter = 0
	for elem in name_list:
		if elem == None:
			query_cols.append(counter)
		counter += 1

	cond = ""
	if len(cond_list) > 0:
		cond = " AND " + " AND ".join(cond_list)

	query = ""
	if len(query_list) > 0:
		query = ", " + ", ".join(query_list)

	cond_raw = ""
	if len(cond_raw_list) > 0:
		cond_raw = " AND " + " AND ".join(cond_raw_list)

	return cond, param_map, query, query_cols, cond_raw

class StructureLayer:
	def __init__(self, engine):
		self.engine = engine
		self.__createSpecificationTable()

	#
	# Specification-oriented functions
	#

	def __createSpecificationTable(self):
		self.engine.execute("CREATE table IF NOT EXISTS '" + _ID_TABLE + "' (id TEXT, field TEXT)")

	def __createSpecification(self, id, fields):
		for field in fields:
			self.__updateSpecification(id, field)

	def __deleteSpecification(self, id):
		self.engine.execute("DELETE FROM '" + _ID_TABLE + "' WHERE id=:id", {'id': id})

	def __updateSpecification(self, id, field):
		name = self.engine.getSafeTableNameFromList(field.name)

		l = list(self.engine.execute("SELECT field FROM '" + _ID_TABLE + "' WHERE id=:id AND field=:field",
			{'id': id, 'field': name}))

		if len(l) == 0:
			self.engine.execute("INSERT INTO '" + _ID_TABLE + "' VALUES (?, ?)", (id, name))

	def getFieldsList(self, id, regexp=None):
		# get object specification
		regexp_cond = ((" AND field REGEXP :field") if regexp != None else "")

		l = list(self.engine.execute("SELECT field FROM '" + _ID_TABLE + "' WHERE id=:id" + regexp_cond,
			{'id': id, 'field': regexp}))

		field_names = [x[0] for x in l]

		return [interface.Field(self.engine.getListFromSafeTableName(field_name)) for field_name in field_names]

	def objectExists(self, id):
		l = list(self.engine.execute("SELECT field FROM '" + _ID_TABLE + "' WHERE id=:id",
			{'id': id}))
		return len(l) > 0

	#
	# Other functions
	#

	def getFieldValue(self, id, field):
		field_name = self.engine.getSafeTableNameFromList(field.name)
		cond, param_map, query, query_cols, cond_raw = _conditionFromList(field.name)

		param_map.update({'id': id})
		l = list(self.engine.execute("SELECT value" + query + " FROM '" + field_name +
			"' WHERE id=:id" + cond, param_map))

		res = []
		for elem in l:
			f = interface.Field(field.name, elem[0])

			counter = 1
			for col in query_cols:
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
		field_name = self.engine.getSafeTableNameFromList(field.name)

		numerical_cols = {}
		numerical_vals = ""
		delete_condition = ""
		counter = 0
		for elem in field.name:
			if isinstance(elem, int):
				numerical_cols['c' + str(counter)] = elem
				numerical_vals += ", :c" + str(counter)

				delete_condition += " AND c" + str(counter) + "=:c" + str(counter)

				counter += 1

		cols_to_insert = {'id': id, 'type': field.type, 'value': field.value}
		cols_to_insert.update(numerical_cols)

		cols_to_delete = {'id': id}
		cols_to_delete.update(numerical_cols)
		delete_condition = "id=:id" + delete_condition

		self.engine.execute("DELETE FROM '" + field_name + "' WHERE " + delete_condition, cols_to_delete)
		self.engine.execute("INSERT INTO '" + field_name + "' VALUES (:id, :type, :value" +
			numerical_vals + ")", cols_to_insert)

	def deleteField(self, id, field):

		field_name = self.engine.getSafeTableNameFromList(field.name)
		cond, param_map, query, query_cols, cond_raw = _conditionFromList(field.name)

		# check if table exists
		if not self.engine.tableExists(field_name):
			return

		# delete value
		self.engine.execute("DELETE FROM '" + field_name + "' WHERE id=:id" + cond_raw,
			{'id': id})

		# check if the table is empty and if it is - delete it too
		if self.engine.tableIsEmpty(field_name):
			self.engine.deleteTable(field_name)

		# if we deleted something from list, we should re-enumerate list elements
		temp_list = list(filter(lambda x: not isinstance(x, str), field.name))
		if len(temp_list) > 0 and temp_list[-1] != None:

			field_cols = list(filter(lambda x: not isinstance(x, str), field.name))
			col_num = len(field_cols) - 1
			col_name = "c" + str(col_num)
			col_val = field_cols[col_num]

			fields_to_reenum = self.getFieldsList(id, field_name)
			for fld in fields_to_reenum:
				self.engine.execute("DELETE FROM '" + self.engine.getSafeTableNameFromList(fld.name) + "' WHERE id=:id " +
					" AND " + col_name + "=" + str(col_val), {'id': id})

				if self.engine.tableIsEmpty(self.engine.getSafeTableNameFromList(fld.name)):
					self.engine.deleteTable(self.engine.getSafeTableNameFromList(fld.name))

				self.engine.execute("UPDATE '" + self.engine.getSafeTableNameFromList(fld.name) + "' SET " +
					col_name + "=" + col_name + "-1 WHERE " +
					"id=:id AND " + col_name + ">=" + str(col_val),
					{'id': id})

	def __assureFieldTableExists(self, field):
		values_str = "id TEXT, type TEXT, value TEXT"
		counter = 0
		for elem in field.name:
			if isinstance(elem, int):
				values_str += ", c" + str(counter) + " INTEGER"
				counter += 1

		self.engine.execute("CREATE TABLE IF NOT EXISTS '" + self.engine.getSafeTableNameFromList(field.name) +
			"' (" + values_str + ")")

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
		return _cleanColsFromList(field.name) in existing_names

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
					return "SELECT * FROM (" + buildSqlQuery(condition.operand1) + \
						") INTERSECT SELECT * FROM (" + buildSqlQuery(condition.operand2) + ")"
				elif isinstance(condition.operator, interface.SearchRequest.Or):
					return "SELECT * FROM (" + buildSqlQuery(condition.operand1) + \
						") UNION SELECT * FROM (" + buildSqlQuery(condition.operand2) + ")"
				else:
					raise Exception("Operator unsupported: " + str(condition.operator))
				return

			field_name = self.engine.getSafeTableNameFromList(condition.operand1.name)
			cond, param_map, query, query_cols, cond_raw = _conditionFromList(condition.operand1.name)

			if not self.engine.tableExists(field_name):
				return self.engine.getEmptyCondition()

			if condition.invert:
				not_str = " NOT "
			else:
				not_str = " "

			if isinstance(condition.operator, interface.SearchRequest.Eq):
				result = "SELECT DISTINCT id FROM '" + field_name + "' WHERE" + not_str + \
					"value = '" + condition.operand2 + "'" + cond_raw
			elif isinstance(condition.operator, interface.SearchRequest.Regexp):
				result = "SELECT DISTINCT id FROM '" + field_name + "' WHERE" + not_str + \
					"value REGEXP '" + condition.operand2 + "'" + cond_raw
			else:
				raise Exception("Comparison unsupported: " + str(condition.operator))

			if condition.invert:
				result = result + " UNION SELECT * FROM (SELECT id FROM '" + _ID_TABLE + \
					"' EXCEPT SELECT id FROM '" + field_name + "')"

			return result

		request = buildSqlQuery(condition)
		result = self.engine.execute(request)
		list_res = [x[0] for x in result]

		return list_res

	def getMaxNumber(self, id, field):
		field_name = self.engine.getSafeTableNameFromList(field.name)
		cond, param_map, query, query_cols, cond_raw = _conditionFromList(field.name)

		# we assume here that all columns in field are defined except for the last one
		query = query[2:] # removing first ','
		param_map.update({'id': id})
		result = self.engine.execute("SELECT MAX (" + query + ") FROM '" + field_name + "' WHERE id=:id" + cond,
			param_map)

		l = list(result)
		res = l[0][0]
		return res

	def reenumerate(self, id, target_field, shift):
		field_name = self.engine.getSafeTableNameFromList(target_field.name)
		field_cols = list(filter(lambda x: isinstance(x, int), target_field.name))
		col_num = len(field_cols) - 1
		col_name = "c" + str(col_num)
		col_val = field_cols[col_num]

		fields_to_reenum = self.getFieldsList(id, field_name)
		for fld in fields_to_reenum:
			self.engine.execute("UPDATE '" + self.engine.getSafeTableNameFromList(fld.name) + "' SET " +
				col_name + "=" + col_name + "+" + str(shift) + " WHERE " +
				"id=:id AND " + col_name + ">=" + str(col_val),
				{'id': id})

class SimpleDatabase(interface.Database):

	def __init__(self, path, engine_class):
		if not issubclass(engine_class, interface.Engine):
			raise Exception("Engine class must be derived from Engine interface")
		self.engine = engine_class(path)
		self.structure = StructureLayer(self.engine)

	def processRequest(self, request):
		if isinstance(request, interface.ModifyRequest):
			self.__processModifyRequest(request.id, request.fields)
		elif isinstance(request, interface.DeleteRequest):
			self.__processDeleteRequest(request.id, request.fields)
		elif isinstance(request, interface.SearchRequest):
			return self.__processSearchRequest(request.condition)
		elif isinstance(request, interface.ReadRequest):
			return self.__processReadRequest(request.id, request.fields)
		elif isinstance(request, interface.InsertRequest):
			self.__processInsertRequest(request.id, request.target_field,
				request.fields, request.one_position)
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
		return result_list

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
