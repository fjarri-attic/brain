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
		#print("Checking for existence: " + name)
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
		existing_fields = self.__getFieldsList(id)
		existing_fields.append(field)

		field_name_list = [_nameFromList(field.name) for field in existing_fields]
		specification = _specificationFromNames(field_name_list)

		self.db.execute("UPDATE '" + _ID_TABLE + "' SET fields=? WHERE id=?",
			(specification, id))

	def __getFieldsList(self, id):
		# get element specification
		l = list(self.db.execute("SELECT fields FROM '" + _ID_TABLE + "' WHERE id=:id",
			{'id': id}))

		specifications = l[0][0]
		field_names = specifications.split('#')

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
		l = list(self.db.execute("SELECT * FROM '" + field_name + "' WHERE id=:id",
			{'id': id}))
		if len(l) > 1:
			raise Exception("Request returned more than one entry")
		elif len(l) == 1:
			# [0]: list contains only one element
			# [1]: return only value, not id
			return l[0][1]
		else:
			return None

	def __updateFieldValue(self, id, field):
		field_name = _nameFromList(field.name)
		self.db.execute("UPDATE '" + field_name + "' SET type=?, contents=?, indexed=? WHERE id=?",
			(field.type, field.value, field.value, id))

	def __setFieldValue(self, id, field):
		self.__assureFieldTableExists(field)
		self.__updateSpecification(id, field)

		field_name = _nameFromList(field.name)
		self.db.execute("INSERT INTO '" + field_name + "' VALUES (:id, :type, :value, :value)",
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
			"' (id TEXT, type TEXT, contents TEXT, indexed TEXT)")

	def createElement(self, id, fields):

		# create element header
		self.__createSpecification(id, fields)

		# update field tables
		for field in fields:
			self.__assureFieldTableExists(field)
			self.__setFieldValue(id, field)

	def deleteElement(self, id):

		fields = self.__getFieldsList(id)

		# for each field, remove it from tables
		for field in fields:
			self.deleteField(id, field)

		self.__deleteSpecification(id)

	def __elementHasField(self, id, field):
		existing_fields = self.__getFieldsList(id)
		existing_names = [existing_field.name for existing_field in existing_fields]
		return field.name in existing_names

	def modifyElement(self, id, fields):

		# for each field, check if it already exists
		for field in fields:
			if self.__elementHasField(id, field):
				self.__updateFieldValue(id, field)
			else:
				self.__setFieldValue(id, field)

class Sqlite3Database(interfaces.Database):

	def __init__(self, path):
		self.db = StructureLayer(path)

	def processRequest(self, request):
		if request.__class__ == interfaces.ModifyRequest:
			self.__processModifyRequest(request)
		elif request.__class__ == interfaces.DeleteRequest:
			self.__processDeleteRequest(request)
		elif request.__class__ == interfaces.SearchRequest:

			def convertFieldNames(condition):
				if condition.leaf:
					condition.operand1 = _nameFromList(condition.operand1)
				else:
					convertFieldNames(condition.operand1)
					convertFieldNames(condition.operand2)

			convertFieldNames(request.condition)
			return self.__processSearchRequest(request)
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

	def __processSearchRequest(self, request):

		def propagateInversion(condition):
			if not condition.leaf:
				condition.operand1.invert = not condition.operand1.invert
				condition.operand2.invert = not condition.operand1.invert

				propagateInversion(condition.operand1)
				propagateInversion(condition.operand2)

				if condition.operator == interfaces.SearchRequest.And:
					condition.operator = interfaces.SearchRequest.Or
				if condition.operator == interfaces.SearchRequest.Or:
					condition.operator = interfaces.SearchRequest.And

		def makeSqlRequest(condition):
			if condition.invert:
				propagateInversion(condition)

			if not condition.leaf:
				if condition.operator == interfaces.SearchRequest.And:
					return "SELECT * FROM (" + makeSqlRequest(condition.operand1) + \
						") INTERSECT SELECT * FROM (" + makeSqlRequest(condition.operand2) + ")"
				elif condition.operator == interfaces.SearchRequest.Or:
					return "SELECT * FROM (" + makeSqlRequest(condition.operand1) + \
						") UNION SELECT * FROM (" + makeSqlRequest(condition.operand2) + ")"
				else:
					raise Exception("Operator unsupported: " + condition.operator.__name__)
				return

			if not self.db.db.tableExists(condition.operand1):
				return "SELECT 0 limit 0" # returns empty result

			if condition.invert:
				not_str = " NOT "
			else:
				not_str = " "

			if condition.operator == interfaces.SearchRequest.Eq:
				result = "SELECT id FROM " + condition.operand1 + " WHERE" + not_str + \
					"contents = '" + condition.operand2 + "'"
			elif condition.operator == interfaces.SearchRequest.Regexp:
				result = "SELECT id FROM " + condition.operand1 + " WHERE" + not_str + \
					"contents REGEXP '" + condition.operand2 + "'"
			else:
				raise Exception("Comparison unsupported: " + condition.operator.__name__)

			if condition.invert:
				result = result + " UNION SELECT * FROM (SELECT id FROM '" + _ID_TABLE + \
					"' EXCEPT SELECT id FROM '" + condition.operand1 + "')"

			return result

		request = makeSqlRequest(request.condition)
		print("Requesting: " + request)
		result = self.db.db.execute(request)
		list_res = [x[0] for x in result]
		#print(repr(list_res))
		return list_res
