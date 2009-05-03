import sqlite3
import interfaces
import re

_FIELD_SEP = '.'
_SPECIFICATION_SEP = '#'
_ID_TABLE = '_id'
_ID_COLUMN = 'id'

def _nameFromList(name_list):
	if isinstance(name_list, str):
		raise Exception("!")
	return _FIELD_SEP.join(name_list)

def _specificationFromNames(name_list):
	return _SPECIFICATION_SEP.join(name_list)
	
class DatabaseWrapper:
	def __init__(self, path):
		self.db = sqlite3.connect(path)
		self.db.create_function("regexp", 2, self.__regexp)
	
	def dump(self):
		print("Dump:")
		for str in self.db.iterdump():
			print(str)
		print("--------")

	def __regexp(self, expr, item):
		r = re.compile(expr)
		return r.match(item) is not None
	
	def execute(self, sql_str, params=None):
		if params:
			return self.db.execute(sql_str, params)
		else:
			return self.db.execute(sql_str)

	def tableExists(self, name):
		res = list(self.db.execute("SELECT name FROM sqlite_master WHERE type='table'"))
		res = [x[0] for x in res]
		#print("Checking for existence: " + name)
		return name in res	

class Sqlite3Database(interfaces.Database):

	__id_column = "_id"

	def __init__(self, path):
		self.db = DatabaseWrapper(path)
		
		# create necessary tables
		self.db.execute("CREATE table IF NOT EXISTS '" + self.__id_column + "' (id TEXT, fields TEXT)")

	def processRequest(self, request):
		if request.__class__ == interfaces.ModifyRequest:
			for field in request.fields:
				field.name = _nameFromList(field.name)
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
		if self.__getFieldValue(self.__id_column, request.id) == None:
			self.__createElements(request.id, request.fields)
		else:
			self.__modifyElements(request.id, request.fields)

	def __createElements(self, id, fields):

		# create element header
		field_name_list = [field.name for field in fields]
		specification = _specificationFromNames(field_name_list)
		self.db.execute("INSERT INTO '" + self.__id_column + "' VALUES (?, ?)", (id, specification))

		# update field tables
		for field in fields:
			self.db.execute("CREATE TABLE IF NOT EXISTS '" + field.name +
				"' (id TEXT, type TEXT, contents TEXT, indexed TEXT)")
			self.__setFieldValue(id, field.name, field.type, field.contents)

	def __getFieldValues(self, field_name, id):
		return list(self.db.execute("SELECT * FROM '" + field_name + "' WHERE id=:id",
			{'id': id}))

	def __getFieldValue(self, field_name, id):
		l = self.__getFieldValues(field_name, id)
		if len(l) > 1:
			raise Exception("Request returned more than one entry")
		elif len(l) == 1:
			return l[0]
		else:
			return None

	def __updateFieldValue(self, id, name, type, value):
		self.db.execute("UPDATE '" + name + "' SET type=?, contents=?, indexed=? WHERE id=?",
			(type, value, value, id))

	def __setFieldValue(self, id, name, type, value):
		self.db.execute("INSERT INTO '" + name + "' VALUES (:id, :type, :value, :value)",
			{'id': id, 'type': type, 'value': value})

	def __deleteFieldValue(self, id, name):

		# check if table exists
		if not self.db.tableExists(name):
			return

		# delete value
		self.db.execute("DELETE FROM '" + name + "' WHERE id=:id",
			{'id': id})

		# check if the table is empty
		if len(list(self.db.execute("SELECT * FROM '" + name + "'"))) == 0:
			self.db.execute("DROP TABLE IF EXISTS '" + name + "'")

	def __modifyElements(self, id, fields):

		# get element specification
		specifications = self.__getFieldValue(self.__id_column, id)[1]
		field_names = specifications.split('#')

		# for each field, check if it already exists
		for field in fields:
			if field.name in field_names:
				self.__updateFieldValue(id, field.name, field.type, field.contents)
			else:
				self.__setFieldValue(id, field.name, field.type, field.contents)

	def __processDeleteRequest(self, request):

		# remove specified fields
		if request.fields != None:
			for field in request.fields:
				field_name = _nameFromList(field.name)
				self.__deleteFieldValue(request.id, field_name)
			return
		
		# delete whole object
		
		# get element specification
		specifications = self.__getFieldValue(self.__id_column, request.id)[1]
		field_names = specifications.split('#')

		# for each field, remove it from tables
		for field_name in field_names:
			self.__deleteFieldValue(request.id, field_name)

		self.__deleteFieldValue(request.id, self.__id_column)

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
			
			if not self.db.tableExists(condition.operand1):
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
				result = result + " UNION SELECT * FROM (SELECT id FROM '" + self.__id_column + \
					"' EXCEPT SELECT id FROM '" + condition.operand1 + "')"

			return result

		request = makeSqlRequest(request.condition)
		print("Requesting: " + request)
		result = self.db.execute(request)
		list_res = [x[0] for x in result]
		#print(repr(list_res))
		return list_res
