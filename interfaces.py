class FormatError(Exception): pass

# Request parser interface
class RequestParser:
	def parseRequest(self, request):
		raise Exception("Not implemented")

# Database interface
class Database:
	def __init__(self, path):
		raise Exception("Not implemented")

	def processRequest(self, request):
		raise Exception("Not implemented")

class Field:
	def __init__(self, name, type=None, value=None):
		self.name = name
		self.type = type
		self.value = value

		if isinstance(self.name, str):
			self.name = [name]
		elif not isinstance(self.name, list):
			raise FormatError("Field name must be a list")

	def __str__(self):
		return "Field ('" + str(self.name) + "'" + \
			(", type=" + str(self.type) if self.type else "") + \
			(", value=" + str(self.value) if self.value else "") + ")"

class _BaseFieldRelatedRequest:
	def __init__(self, id, fields=None):
		self.id = id
		self.fields = fields

		if self.fields != None and not isinstance(fields, list):
			raise FormatError("Data should be list of fields")

	def getFieldsStr(self):
		if self.fields:
			return ", ".join([str(field) for field in self.fields])
		else:
			return ""

class ModifyRequest(_BaseFieldRelatedRequest):
	def __init__(self, id, fields=None):
		_BaseFieldRelatedRequest.__init__(self, id, fields)
		for field in self.fields:
			if not isinstance(field, Field):
				raise FormatError("Field list element must have class Field")

	def __str__(self):
		return "ModifyRequest for element '" + self.id + "': " + self.getFieldsStr()

class DeleteRequest(_BaseFieldRelatedRequest):
	def __str__(self):
		return "DeleteRequest for element '" + self.id + "': " + self.getFieldsStr()

class ReadRequest(_BaseFieldRelatedRequest):
	def __str__(self):
		return "DeleteRequest for element '" + self.id + "': " + self.__getFieldsStr()

class SearchRequest:

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
