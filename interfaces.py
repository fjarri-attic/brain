"""Interface for database layer"""

class FormatError(Exception):
	"""Request format error exception"""
	pass

class Database:
	"""Database layer class interface"""

	def processRequest(self, request):
		raise Exception("Not implemented")

class Field:
	"""Structure, representing object field"""

	def __init__(self, name, value=None):
		self.type = 'text' # hardcoded now
		self.value = value

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

		# clone given list
		self.name = name[:]

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
		self.id = id

		if fields != None and not isinstance(fields, list):
			raise FormatError("Data should be a list")

		if fields != None:
			for field in fields:
				if not isinstance(field, Field):
					raise FormatError("Data should be a list of Field objects")

		self.fields = fields

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
		_BaseRequest.__init__(self, id, fields)

		if not isinstance(target_field, Field):
			raise FormatError("Target field must have class Field")

		self.target_field = target_field
		self.one_position = one_position

	def __str__(self):
		return _BaseRequest.__str__(self) + ", target: " + str(self.target_field)

class SearchRequest:
	"""Request for searching in database"""

	# Helper classes

	class Operator: pass
	class Comparison: pass

	class And(Operator):
		def __str__(self): return "And"

	class Or(Operator):
		def __str__(self): return "Or"

	class Eq(Comparison):
		def __str__(self): return "=="

	class Regexp(Comparison):
		def __str__(self): return "=~"

	class Condition:
		"""Class for main element of search request"""

		def __init__(self, operand1, operator, operand2, invert=False):

			if isinstance(operator, SearchRequest.Comparison):
				self.leaf = True
			elif isinstance(operator, SearchRequest.Operator):
				if not isinstance(operand1, SearchRequest.Condition):
					raise FormatError("Wrong condition type: " + operand1.__class__.__name__)
				if not isinstance(operand2, SearchRequest.Condition):
					raise FormatError("Wrong condition type: " + operand2.__class__.__name__)
				self.leaf = False
			else:
				raise FormatError("Wrong operator type: " + operator.__class__.__name__)

			if self.leaf and not isinstance(operand1, Field):
				raise FormatError("First operand should be Field, but it is " +
					operand1.__class__.__name__)

			self.operand1 = operand1
			self.operand2 = operand2
			self.operator = operator
			self.invert = invert

		def __str__(self):
			return "(" + str(self.operand1) + " " + \
				("!" if self.invert else "") + str(self.operator) + \
				" " + str(self.operand2) + ")"

	def __init__(self, condition):
		if not condition.__class__ == self.Condition:
			raise FormatError("Wrong condition type: " + condition.__class__.__name__)

		self.condition = condition

	def __str__(self):
		return "SearchRequest: " + str(self.condition)
