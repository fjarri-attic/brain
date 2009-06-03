"""Interface for database layer"""

import copy

#
# Exceptions
#

class FormatError(Exception):
	"""Request format error exception"""
	pass

#
# Classes
#

class Engine:
	"""Engine layer class interface"""

	def dump(self):
		"""Dump the whole database to string; used for debug purposes"""
		raise Exception("Not implemented")

	def execute(self, sql_str, params=None): raise Exception("Not implemented")
	def tableExists(self, name): raise Exception("Not implemented")
	def tableIsEmpty(self, name): raise Exception("Not implemented")
	def deleteTable(self, name): raise Exception("Not implemented")

	def getEmptyCondition(self):
		"""Returns condition for compound SELECT which evaluates to empty table"""
		raise Exception("Not implemented")

	def getSafeValue(self, s):
		"""Transform string value so that it could be safely used in queries"""
		raise Exception("Not implemented")

	def getNameString(self, l):
		"""Get field name from list"""
		raise Exception("Not implemented")

	def getNameList(self, s):
		"""Get field name list from string"""
		raise Exception("Not implemented")

	def getSafeName(self, s):
		"""Transform string value so that it could be safely used as table name"""
		raise Exception("Not implemented")

class Database:
	"""Database layer class interface"""

	def processRequest(self, request):
		raise Exception("Not implemented")

#
# Requests
#

class Field:
	"""Structure, representing object field"""

	def __init__(self, name, value=None):

		# check given name
		if name == None or name == '':
			raise FormatError("Field name cannot be empty")

		if not isinstance(name, list):
			name = [name]

		# check that list contains only strings, ints and Nones
		for elem in name:
			if not isinstance(elem, str) and \
					not isinstance(elem, int) and \
					not elem == None:
				raise FormatError("Field name list must contain only integers, strings or Nones")

		# initialize fields
		self.type = 'text' # hardcoded now
		self.value = value
		self.name = copy.deepcopy(name)

	def __eq__(self, other):
		if other == None:
			return False

		if not isinstance(other, Field):
			return False

		return (self.name == other.name) and (self.type == other.type) and (self.value == other.value)

	def __str__(self):
		return "Field ('" + str(self.name) + "'" + \
			(", type=" + str(self.type) if self.type else "") + \
			(", value=" + str(self.value) if self.value else "") + ")"

	def __repr__(self):
		return str(self)

class _BaseRequest:
	"""Base class for request with common format checks"""

	def __init__(self, id, fields=None):

		if fields != None and not isinstance(fields, list):
			raise FormatError("Data should be a list")

		if fields != None:
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

	def __init__(self, id, target_field, fields, one_position=False):

		# target field should be an object of Field
		if not isinstance(target_field, Field):
			raise FormatError("Target field must have class Field")

		# target field should be determined, except maybe for the last element
		for elem in target_field.name[:-1]:
			if elem == None:
				raise FormatError("Target field should not have None parts in name, " +
					"except for the last one")

		# target field should point on list
		if target_field.name[-1] != None and not isinstance(target_field.name[-1], int):
			raise FormatError("Last element of target field name should be None or integer")

		# all fields to insert should be fully determined
		for field in fields:
			for elem in field.name:
				if elem == None:
					raise FormatError("Each of fields to insert should be determined")

		# Initialize fields
		_BaseRequest.__init__(self, id, fields)
		self.target_field = copy.deepcopy(target_field)
		self.one_position = one_position

	def __str__(self):
		return _BaseRequest.__str__(self) + ", target: " + str(self.target_field)

class SearchRequest:
	"""Request for searching in database"""

	# Operator classes

	class Operator: pass

	class And(Operator):
		def __str__(self): return "And"

	class Or(Operator):
		def __str__(self): return "Or"

	# Comparison classes

	class Comparison: pass

	class Eq(Comparison):
		def __str__(self): return "=="

	class Regexp(Comparison):
		def __str__(self): return "=~"

	class Condition:
		"""Class for main element of search request"""

		def __init__(self, operand1, operator, operand2, invert=False):

			if isinstance(operator, SearchRequest.Comparison):
				# if node operator is a Comparison, it is a leaf of condition tree
				if not isinstance(operand1, Field):
					raise FormatError("First operand should be Field, but it is " +
						operand1.__class__.__name__)
				self.leaf = True
			elif isinstance(operator, SearchRequest.Operator):
				# if node operator is an Operator, both operands should be Conditions
				if not isinstance(operand1, SearchRequest.Condition):
					raise FormatError("Wrong condition type: " + operand1.__class__.__name__)
				if not isinstance(operand2, SearchRequest.Condition):
					raise FormatError("Wrong condition type: " + operand2.__class__.__name__)
				self.leaf = False
			else:
				raise FormatError("Wrong operator type: " + operator.__class__.__name__)

			# Initialize fields
			# Using deepcopy for reliability, because we do not know which part
			# of complex condition is reusable; user can store and change conditions
			# without creating SearchRequest
			self.operand1 = copy.deepcopy(operand1)
			self.operand2 = copy.deepcopy(operand2)
			self.operator = operator
			self.invert = invert

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
