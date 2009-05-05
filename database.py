import sqlite3
import interfaces
import re

_FIELD_SEP = '.'
_SPECIFICATION_SEP = '#'
_ID_TABLE = 'id'
_ID_COLUMN = 'id'

def _nameFromList(name_list):
	return "_" + _FIELD_SEP.join(name_list)

def _listFromName(name):
	return name[1:].split(_FIELD_SEP)

def _specificationFromNames(name_list):
	return _SPECIFICATION_SEP.join(name_list)

def _namesFromSpecification(specification):
	return specification.split(_SPECIFICATION_SEP)

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
		return r.match(item) is not None

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
		self.db.execute("CREATE table IF NOT EXISTS '" + _ID_TABLE + "' (id TEXT, fields TEXT)")

	def __createSpecification(self, id, fields):
		field_name_list = [_nameFromList(field.name) for field in fields]
		specification = _specificationFromNames(field_name_list)
		self.db.execute("INSERT INTO '" + _ID_TABLE + "' VALUES (?, ?)", (id, specification))

	def __deleteSpecification(self, id):
		self.db.execute("DELETE FROM '" + _ID_TABLE + "' WHERE id=:id",	{'id': id})

	def __updateSpecification(self, id, field):
		existing_fields = self.getFieldsList(id)
		existing_fields.append(field)

		field_name_list = [_nameFromList(field.name) for field in existing_fields]
		specification = _specificationFromNames(field_name_list)

		self.db.execute("UPDATE '" + _ID_TABLE + "' SET fields=? WHERE id=?",
			(specification, id))

	def getFieldsList(self, id):
		# get element specification
		l = list(self.db.execute("SELECT fields FROM '" + _ID_TABLE + "' WHERE id=:id",
			{'id': id}))

		specification = l[0][0]
		field_names = _namesFromSpecification(specification)

		return [interfaces.Field(_listFromName(field_name)) for field_name in field_names]

	def elementExists(self, id):
		l = list(self.db.execute("SELECT fields FROM '" + _ID_TABLE + "' WHERE id=:id",
			{'id': id}))
		return len(l) > 0

	#
	# Other functions
	#

	def getFieldValue(self, id, field):
		field_name = _nameFromList(field.name)
		l = list(self.db.execute("SELECT value FROM '" + field_name + "' WHERE id=:id",
			{'id': id}))
		if len(l) > 1:
			raise Exception("Request returned more than one entry")
		elif len(l) == 1:
			# [0]: list contains only one element
			# [0]: each list element is a tuple with one value
			return interfaces.Field(field.name, 'text', l[0][0])
		else:
			return None

	def __updateFieldValue(self, id, field):
		field_name = _nameFromList(field.name)
		self.db.execute("UPDATE '" + field_name + "' SET type=?, value=? WHERE id=?",
			(field.type, field.value, id))

	def __setFieldValue(self, id, field):
		self.__assureFieldTableExists(field)
		field_name = _nameFromList(field.name)

		self.db.execute("DELETE FROM '" + field_name + "' WHERE id=:id", {'id': id})
		self.db.execute("INSERT INTO '" + field_name + "' VALUES (:id, :type, :value)",
			{'id': id, 'type': field.type, 'value': field.value})

	def deleteField(self, id, field):

		field_name = _nameFromList(field.name)

		# check if table exists
		if not self.db.tableExists(field_name):
			return

		# delete value
		self.db.execute("DELETE FROM '" + field_name + "' WHERE id=:id",
			{'id': id})

		# check if the table is empty and if it is - delete it too
		if self.db.tableIsEmpty(field_name):
			self.db.deleteTable(field_name)

	def __assureFieldTableExists(self, field):
		self.db.execute("CREATE TABLE IF NOT EXISTS '" + _nameFromList(field.name) +
			"' (id TEXT, type TEXT, value TEXT)")

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
		return field.name in existing_names

	def modifyElement(self, id, fields):

		# for each field, check if it already exists
		for field in fields:
			if self.elementHasField(id, field):
				self.__updateFieldValue(id, field)
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

			if not self.db.tableExists(field_name):
				return "SELECT 0 limit 0" # returns empty result

			if condition.invert:
				not_str = " NOT "
			else:
				not_str = " "

			if isinstance(condition.operator, interfaces.SearchRequest.Eq):
				result = "SELECT id FROM " + field_name + " WHERE" + not_str + \
					"value = '" + condition.operand2 + "'"
			elif isinstance(condition.operator, interfaces.SearchRequest.Regexp):
				result = "SELECT id FROM " + field_name + " WHERE" + not_str + \
					"value REGEXP '" + condition.operand2 + "'"
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
		else:
			raise Exception("Unknown request type: " + request.__class__.__name__)

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

		return [self.db.getFieldValue(request.id, field) for field in fields_to_read]

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
