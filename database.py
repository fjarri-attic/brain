import sqlite3
import interfaces
import re

_FIELD_SEP = '.'
_ID_TABLE = 'id'
_ID_COLUMN = 'id'

def _nameFromList(name_list):
	temp_list = [(x if isinstance(x, str) else '') for x in name_list]
	return "_" + _FIELD_SEP.join(temp_list)

def _listFromName(name):
	return [(x if x != '' else None) for x in name[1:].split(_FIELD_SEP)]

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

class DatabaseLayer:
	def __init__(self, path):
		self.__conn = sqlite3.connect(path)
		self.__conn.create_function("regexp", 2, self.__regexp)

	def dump(self):
		print("Dump:")
		for str in self.__conn.iterdump():
			print(str)
		print("--------")

	def __regexp(self, expr, item):
		r = re.compile(expr)
		return r.search(item) is not None

	def execute(self, sql_str, params=None):
		if params:
			return self.__conn.execute(sql_str, params)
		else:
			return self.__conn.execute(sql_str)

	def tableExists(self, name):
		res = list(self.__conn.execute("SELECT name FROM sqlite_master WHERE type='table'"))
		res = [x[0] for x in res]
		return name in res

	def tableIsEmpty(self, name):
		return len(list(self.__conn.execute("SELECT * FROM '" + name + "'"))) == 0

	def deleteTable(self, name):
		self.__conn.execute("DROP TABLE IF EXISTS '" + name + "'")

class StructureLayer:
	def __init__(self, path):
		self.db = DatabaseLayer(path)
		self.__createSpecificationTable()

	#
	# Specification-oriented functions
	#

	def __createSpecificationTable(self):
		self.db.execute("CREATE table IF NOT EXISTS '" + _ID_TABLE + "' (id TEXT, field TEXT)")

	def __createSpecification(self, id, fields):
		for field in fields:
			self.__updateSpecification(id, field)

	def __deleteSpecification(self, id):
		self.db.execute("DELETE FROM '" + _ID_TABLE + "' WHERE id=:id",	{'id': id})

	def __updateSpecification(self, id, field):
		name = _nameFromList(field.name)

		l = list(self.db.execute("SELECT field FROM '" + _ID_TABLE + "' WHERE id=:id AND field=:field",
			{'id': id, 'field': name}))

		if len(l) == 0:
			self.db.execute("INSERT INTO '" + _ID_TABLE + "' VALUES (?, ?)", (id, name))

	def getFieldsList(self, id, regexp=None):
		# get element specification
		regexp_cond = ((" AND field REGEXP :field") if regexp != None else "")

		l = list(self.db.execute("SELECT field FROM '" + _ID_TABLE + "' WHERE id=:id" + regexp_cond,
			{'id': id, 'field': regexp}))

		field_names = [x[0] for x in l]

		return [interfaces.Field(_listFromName(field_name)) for field_name in field_names]

	def elementExists(self, id):
		l = list(self.db.execute("SELECT field FROM '" + _ID_TABLE + "' WHERE id=:id",
			{'id': id}))
		return len(l) > 0

	#
	# Other functions
	#

	def getFieldValue(self, id, field):
		field_name = _nameFromList(field.name)
		cond, param_map, query, query_cols, cond_raw = _conditionFromList(field.name)

		param_map.update({'id': id})
		l = list(self.db.execute("SELECT value" + query + " FROM '" + field_name +
			"' WHERE id=:id" + cond, param_map))

		res = []
		for elem in l:
			f = interfaces.Field(field.name, 'text', elem[0])

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
		field_name = _nameFromList(field.name)

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

		self.db.execute("DELETE FROM '" + field_name + "' WHERE " + delete_condition, cols_to_delete)
		self.db.execute("INSERT INTO '" + field_name + "' VALUES (:id, :type, :value" +
			numerical_vals + ")", cols_to_insert)

	def deleteField(self, id, field):

		field_name = _nameFromList(field.name)
		cond, param_map, query, query_cols, cond_raw = _conditionFromList(field.name)

		# check if table exists
		if not self.db.tableExists(field_name):
			return

		# delete value
		self.db.execute("DELETE FROM '" + field_name + "' WHERE id=:id" + cond_raw,
			{'id': id})

		# check if the table is empty and if it is - delete it too
		if self.db.tableIsEmpty(field_name):
			self.db.deleteTable(field_name)

		# if we deleted something from list, we should re-enumerate list elements
		if cond != "":

			field_cols = list(filter(lambda x: isinstance(x, int), field.name))
			col_num = len(field_cols) - 1
			col_name = "c" + str(col_num)
			col_val = field_cols[col_num]

			fields_to_reenum = self.getFieldsList(id, field_name)
			for fld in fields_to_reenum:
				self.db.execute("DELETE FROM '" + _nameFromList(fld.name) + "' WHERE id=:id " +
					" AND " + col_name + "=" + str(col_val), {'id': id})

				if self.db.tableIsEmpty(_nameFromList(fld.name)):
					self.db.deleteTable(_nameFromList(fld.name))

				self.db.execute("UPDATE '" + _nameFromList(fld.name) + "' SET " +
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

		self.db.execute("CREATE TABLE IF NOT EXISTS '" + _nameFromList(field.name) +
			"' (" + values_str + ")")

	def createElement(self, id, fields):

		# create element header
		self.__createSpecification(id, fields)

		# update field tables
		for field in fields:
			self.__assureFieldTableExists(field)
			self.__setFieldValue(id, field)

	def deleteElement(self, id):

		fields = self.getFieldsList(id)

		# for each field, remove it from tables
		for field in fields:
			self.deleteField(id, field)

		self.__deleteSpecification(id)

	def elementHasField(self, id, field):
		existing_fields = self.getFieldsList(id)
		existing_names = [existing_field.name for existing_field in existing_fields]
		return _cleanColsFromList(field.name) in existing_names

	def modifyElement(self, id, fields):

		# for each field, check if it already exists
		for field in fields:
			if self.elementHasField(id, field):
				self.__setFieldValue(id, field)
			else:
				self.__updateSpecification(id, field)
				self.__setFieldValue(id, field)

	def searchForElements(self, condition):
		def buildSqlQuery(condition):

			if not condition.leaf:
				if isinstance(condition.operator, interfaces.SearchRequest.And):
					return "SELECT * FROM (" + buildSqlQuery(condition.operand1) + \
						") INTERSECT SELECT * FROM (" + buildSqlQuery(condition.operand2) + ")"
				elif isinstance(condition.operator, interfaces.SearchRequest.Or):
					return "SELECT * FROM (" + buildSqlQuery(condition.operand1) + \
						") UNION SELECT * FROM (" + buildSqlQuery(condition.operand2) + ")"
				else:
					raise Exception("Operator unsupported: " + str(condition.operator))
				return

			field_name = _nameFromList(condition.operand1.name)
			cond, param_map, query, query_cols, cond_raw = _conditionFromList(condition.operand1.name)

			if not self.db.tableExists(field_name):
				return "SELECT 0 limit 0" # returns empty result

			if condition.invert:
				not_str = " NOT "
			else:
				not_str = " "

			if isinstance(condition.operator, interfaces.SearchRequest.Eq):
				result = "SELECT DISTINCT id FROM '" + field_name + "' WHERE" + not_str + \
					"value = '" + condition.operand2 + "'" + cond_raw
			elif isinstance(condition.operator, interfaces.SearchRequest.Regexp):
				result = "SELECT DISTINCT id FROM '" + field_name + "' WHERE" + not_str + \
					"value REGEXP '" + condition.operand2 + "'" + cond_raw
			else:
				raise Exception("Comparison unsupported: " + str(condition.operator))

			if condition.invert:
				result = result + " UNION SELECT * FROM (SELECT id FROM '" + _ID_TABLE + \
					"' EXCEPT SELECT id FROM '" + field_name + "')"

			return result

		request = buildSqlQuery(condition)
		result = self.db.execute(request)
		list_res = [x[0] for x in result]

		return list_res

	def getMaxNumber(self, id, field):
		field_name = _nameFromList(field.name)
		cond, param_map, query, query_cols, cond_raw = _conditionFromList(field.name)

		# we assume here that all columns in field are defined except for the last one
		cond = cond[2:] # removing first ','
		result = self.db.execute("SELECT MAX (" + cond + ") FROM '" + field_name + "' WHERE " + cond,
			param_map)

		l = list(result)
		res = l[0][0]
		return res

	def reenumerate(self, id, target_field, shift):
		field_name = _nameFromList(target_field.name)
		field_cols = list(filter(lambda x: isinstance(x, int), target_field.name))
		col_num = len(field_cols) - 1
		col_name = "c" + str(col_num)
		col_val = field_cols[col_num]

		fields_to_reenum = self.getFieldsList(id, field_name)
		for fld in fields_to_reenum:
			self.db.execute("UPDATE '" + _nameFromList(fld.name) + "' SET " +
				col_name + "=" + col_name + "+" + str(shift) + " WHERE " +
				"id=:id AND " + col_name + ">=" + str(col_val),
				{'id': id})

class Sqlite3Database(interfaces.Database):

	def __init__(self, path):
		self.db = StructureLayer(path)

	def processRequest(self, request):
		if isinstance(request, interfaces.ModifyRequest):
			self.__processModifyRequest(request)
		elif isinstance(request, interfaces.DeleteRequest):
			self.__processDeleteRequest(request)
		elif isinstance(request, interfaces.SearchRequest):
			return self.__processSearchRequest(request)
		elif isinstance(request, interfaces.ReadRequest):
			return self.__processReadRequest(request)
		elif isinstance(request, interfaces.InsertRequest):
			return self.__processInsertRequest(request)
		else:
			raise Exception("Unknown request type: " + request.__class__.__name__)

	def __processInsertRequest(self, request):

		def enumerate(fields, col_num, starting_num, one_position=False):
			counter = starting_num
			for field in request.fields:
				field.name[col_num] = counter
				if not one_position:
					counter += 1

		if not self.db.elementExists(request.id):
			raise Exception("Element " + request.id + " does not exist")

		target_col = len(request.target_field.name) - 1 # last column in name of target field

		if not self.db.elementHasField(request.id, request.target_field):
			enumerate(request.fields, target_col, 0, request.one_position)
		elif request.target_field.name[target_col] == None:
			starting_num = self.db.getMaxNumber(request.id, request.target_field) + 1
			enumerate(request.fields, target_col, starting_num, request.one_position)
		else:
			self.db.reenumerate(request.id, request.target_field,
				(1 if request.one_position else len(request.fields)))
			enumerate(request.fields, target_col, request.target_field.name[target_col], request.one_position)

		self.__processModifyRequest(interfaces.ModifyRequest(
			request.id, request.fields
		))
		
	def __processModifyRequest(self, request):

		# check if the entry with specified id already exists
		# if no, just add it to the database
		if not self.db.elementExists(request.id):
			self.db.createElement(request.id, request.fields)
		else:
			self.db.modifyElement(request.id, request.fields)

	def __processDeleteRequest(self, request):

		if request.fields != None:
			# remove specified fields
			for field in request.fields:
				self.db.deleteField(request.id, field)
			return
		else:
			# delete whole object
			self.db.deleteElement(request.id)

	def __processReadRequest(self, request):
		if request.fields:
			fields_to_read = filter(lambda x: self.db.elementHasField(request.id, x), request.fields)
		else:
			fields_to_read = self.db.getFieldsList(request.id)

		results = [self.db.getFieldValue(request.id, field) for field in fields_to_read]

		result_list = []
		for result in results:
			if result != None:
				result_list += result
		return result_list

	def __processSearchRequest(self, request):

		def propagateInversion(condition):
			if not condition.leaf:
				if condition.invert:

					condition.invert = False

					condition.operand1.invert = not condition.operand1.invert
					condition.operand2.invert = not condition.operand2.invert

					if isinstance(condition.operator, interfaces.SearchRequest.And):
						condition.operator = interfaces.SearchRequest.Or()
					elif isinstance(condition.operator, interfaces.SearchRequest.Or):
						condition.operator = interfaces.SearchRequest.And()

				propagateInversion(condition.operand1)
				propagateInversion(condition.operand2)

		propagateInversion(request.condition)

		return self.db.searchForElements(request.condition)
