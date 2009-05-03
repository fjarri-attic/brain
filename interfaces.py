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

class _BaseWriteRequest:
	def __init__(self, id, fields=None):
		self.id = id
		self.fields = fields

		if self.fields != None and not isinstance(fields, list):
			raise FormatError("Data should be list of fields")

class ModifyRequest(_BaseWriteRequest):
	def __init__(self, id, fields=None):
		_BaseWriteRequest.__init__(self, id, fields)
		for field in self.fields:
			if not isinstance(field, Field):
				raise FormatError("Field list element must have class Field")

class DeleteRequest(_BaseWriteRequest): pass


class Field:
	def __init__(self, name, type=None, value=None):
		self.name = name
		self.type = type
		self.value = value

		if isinstance(self.name, str):
			self.name = [name]
		elif not isinstance(self.name, list):
			raise FormatError("Field name must be a list")

class SearchRequest:

	class Operator: pass
	class Comparison: pass

	class And(Operator): pass
	class Or(Operator): pass

	class Eq(Comparison): pass
	class Regexp(Comparison): pass

	class Condition:
		def __init__(self, operand1, operator, operand2, invert=False):

			if issubclass(operator, SearchRequest.Comparison):
				self.leaf = True
			elif issubclass(operator, SearchRequest.Operator):
				if not issubclass(operand1.__class__, SearchRequest.Condition):
					raise FormatError("Wrong condition type: " + operand1.__class__.__name__)
				if not issubclass(operand2.__class__, SearchRequest.Condition):
					raise FormatError("Wrong condition type: " + operand2.__class__.__name__)
				self.leaf = False
			else:
				raise FormatError("Wrong operator type: " + operator.__name__)

			if self.leaf and not isinstance(operand1, Field):
				raise FormatError("First operand should be Field, but it is " +
					operand1.__class__.__name__)

			self.operand1 = operand1
			self.operand2 = operand2
			self.operator = operator
			self.invert = invert

	def __init__(self, condition):
		if not condition.__class__ == self.Condition:
			raise FormatError("Wrong condition type: " + condition.__class__.__name__)

		self.condition = condition
