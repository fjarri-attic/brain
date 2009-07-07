"""Interface for database layer"""

import copy

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

from brain.field import Field
import brain.op as op

#
# Exceptions
#

class BrainError(Exception):
	"""Base class for brain exceptions"""
	pass

class FormatError(BrainError):
	"""Request format error exception"""
	pass

class LogicError(BrainError):
	"""Signals an error in logic layer"""
	pass

class StructureError(BrainError):
	"""Signals an error in structure layer"""
	pass

class FacadeError(BrainError):
	"""Signals an error in facade layer"""
	pass

#
# Classes
#

class Engine:
	"""Engine layer class interface"""

	def disconnect(self):
		raise NotImplementedError

	def dump(self):
		"""Dump the whole database to string; used for debug purposes"""
		raise NotImplementedError

	def execute(self, sql_str):
		"""Execute given SQL query"""
		raise NotImplementedError

	def getNewId(self):
		"""Return new unique ID for this database"""
		raise NotImplementedError

	def getIdType(self):
		"""Return type string for IDs used in this database"""
		raise NotImplementedError

	def tableExists(self, name): raise NotImplementedError
	def tableIsEmpty(self, name): raise NotImplementedError
	def deleteTable(self, name): raise NotImplementedError

	def getEmptyCondition(self):
		"""Returns condition for compound SELECT which evaluates to empty table"""
		raise NotImplementedError

	def getSafeValue(self, s):
		"""Transform string value so that it could be safely used in queries"""
		raise NotImplementedError

	def getColumnType(self, val):
		"""Return SQL type for storing given value"""
		raise NotImplementedError

	def getValueClass(self, type_str):
		"""Return Python class for the given SQL type"""
		raise NotImplementedError

	def getNameString(self, l):
		"""Get field name from list"""
		raise NotImplementedError

	def getNameList(self, s):
		"""Get field name list from string"""
		raise NotImplementedError

	def getSafeName(self, s):
		"""Transform string value so that it could be safely used as table name"""
		raise NotImplementedError

	def getNullValue(self):
		"""Returns null value to use in queries"""
		raise NotImplementedError

	def begin(self):
		"""Begin transaction"""
		raise NotImplementedError

	def commit(self):
		"""Commit current transaction"""
		raise NotImplementedError

	def rollback(self):
		"""Rollback current transaction"""
		raise NotImplementedError

class Database:
	"""Database layer class interface"""

	def processRequest(self, request):
		raise NotImplementedError
#
# Requests
#

class _BaseRequest:
	"""Base class for request with common format checks"""

	def __init__(self, id, fields=None):

		if fields is not None and not isinstance(fields, list):
			raise FormatError("Data should be a list")

		if fields is not None:
			for field in fields:
				if not isinstance(field, Field):
					raise FormatError("Data should be a list of Field objects")

		# Initialize fields
		self.id = id
		self.fields = copy.deepcopy(fields)

	def __str__(self):
		return self.__class__.__name__ + " for object '" + self.id + "': " + str(self.fields)


class ModifyRequest(_BaseRequest):
	"""Request for modification of existing objects or adding new ones"""
	pass

class DeleteRequest(_BaseRequest):
	"""Request for deletion of existing objects"""
	pass

class ReadRequest(_BaseRequest):
	"""Request for reading existing objects"""
	pass

class InsertRequest(_BaseRequest):
	"""Request for insertion into list of fields"""

	def __init__(self, id, target_field, fields):

		# target field should be an object of Field
		if not isinstance(target_field, Field):
			raise FormatError("Target field must have class Field")

		# target field should be determined, except maybe for the last element
		for elem in target_field.name[:-1]:
			if elem is None:
				raise FormatError("Target field should not have None parts in name, " +
					"except for the last one")

		# target field should point on list
		if target_field.name[-1] is not None and not isinstance(target_field.name[-1], int):
			raise FormatError("Last element of target field name should be None or integer")

		# all fields to insert should be fully determined
		for field in fields:
			for elem in field.name:
				if elem is None:
					raise FormatError("Each of fields to insert should be determined")

		# Initialize fields
		_BaseRequest.__init__(self, id, fields)
		self.target_field = copy.deepcopy(target_field)

	def __str__(self):
		return _BaseRequest.__str__(self) + ", target: " + str(self.target_field)

class InsertManyRequest:
	def __init__(self, id, target_field, field_groups):
		self.id = id
		self.target_field = target_field
		self.field_groups = field_groups

class SearchRequest:
	"""Request for searching in database"""

	class Condition:
		"""Class for main element of search request"""

		def __init__(self, operand1, operator, operand2, invert=False, leaf=False):
			self.operand1 = operand1
			self.operand2 = operand2
			self.operator = operator
			self.invert = invert
			self.leaf = leaf

		def __str__(self):
			return "(" + str(self.operand1) + " " + \
				("!" if self.invert else "") + str(self.operator) + \
				" " + str(self.operand2) + ")"

	def __init__(self, condition):
		if not condition.__class__ == self.Condition:
			raise FormatError("Wrong condition type: " + condition.__class__.__name__)

		self.condition = copy.deepcopy(condition)

	def __str__(self):
		return "SearchRequest: " + str(self.condition)
