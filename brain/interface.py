"""Interface for database layer"""

import copy

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

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


SUPPORTED_TYPES = [
	int,
	str,
	float,
	bytes
]

class Field:
	"""Class for more convenient handling of Field objects"""

	def __init__(self, engine, name, value=None):
		self._engine = engine
		self.name = name[:]
		self.value = value

	@classmethod
	def fromNameStr(cls, engine, name_str, value=None):
		"""Create object using stringified name instead of list"""

		# cut prefix 'field' from the resulting list
		return cls(engine, engine.getNameList(name_str)[1:], value)

	def ancestors(self, include_self):
		"""
		Iterate through all ancestor fields
		Yields tuple (ancestor, last removed name part)
		"""
		name_copy = self.name[:]
		last = name_copy.pop() if not include_self else None
		while len(name_copy) > 0:
			yield Field(self._engine, name_copy), last
			last = name_copy.pop()

	def _getListColumnName(self, index):
		"""Get name of additional list column corresponding to given index"""
		return "c" + str(index)

	def isNull(self):
		"""Whether field contains Null value"""
		return (self.value is None)

	def __get_type_str(self):
		"""Returns string with SQL type for stored value"""
		return self._engine.getColumnType(self.value) if not self.isNull() else None

	def __set_type_str(self, type_str):
		"""Set field type using given value from specification table"""
		if type_str is None:
			self.value = None
		else:
			self.value = self._engine.getValueClass(type_str)()

	type_str = property(__get_type_str, __set_type_str)

	@property
	def type_str_as_value(self):
		"""Returns string with SQL type for stored value"""
		if not self.isNull():
			return self._engine.getSafeValue(self.type_str)
		else:
			return self._engine.getNullValue()

	@property
	def name_str_no_type(self):
		"""Returns name string with no type specifier"""
		return self._engine.getNameString(['field'] + self.name)

	@property
	def safe_value(self):
		"""Returns value in form that can be safely used as value in queries"""
		return self._engine.getSafeValue(self.value)

	@property
	def name_str(self):
		"""Returns field name in string form"""
		return self._engine.getNameString(['field', self.type_str] + self.name)

	@property
	def name_as_table(self):
		"""Returns field name in form that can be safely used as a table name"""
		return self._engine.getSafeName(self.name_str)

	@property
	def name_as_value(self):
		"""Returns field name in form that can be safely used as value in queries"""
		return self._engine.getSafeValue(self.name_str)

	@property
	def name_as_value_no_type(self):
		return self._engine.getSafeValue(self.name_str_no_type)

	@property
	def columns_query(self):
		"""Returns string with additional values list necessary to query the value of this field"""
		numeric_columns = filter(lambda x: not isinstance(x, str), self.name)
		counter = 0
		l = []
		for column in numeric_columns:
			if column is None:
				l.append(self._getListColumnName(counter))
			counter += 1

		# if value is null, this condition will be used alone,
		# so there's no need in leading comma
		return (('' if self.isNull() else ', ') + ', '.join(l) if len(l) > 0 else '')

	@property
	def columns_condition(self):
		"""Returns string with condition for operations on given field"""

		# do not skip Nones, because we need them for
		# getting proper index of list column
		numeric_columns = filter(lambda x: not isinstance(x, str), self.name)
		counter = 0
		l = []
		for column in numeric_columns:
			if column is not None:
				l.append(self._getListColumnName(counter) +
					"=" + str(column))
			counter += 1

		return (' AND '.join([''] + l) if len(l) > 0 else '')

	def getDeterminedName(self, vals):
		"""Returns name with Nones filled with supplied list of values"""
		vals_copy = list(vals)
		func = lambda x: vals_copy.pop(0) if x is None else x
		return list(map(func, self.name))

	def getCreationStr(self, id_column, value_column, id_type, list_index_type):
		"""Returns string containing list of columns necessary to create field table"""
		counter = 0
		res = ""
		for elem in self.name:
			if not isinstance(elem, str):
				res += ", " + self._getListColumnName(counter) + " " + list_index_type
				counter += 1

		return ("{id_column} {id_type}" +
			(", {value_column} {value_type}" if not self.isNull() else "") + res).format(
			id_column=id_column,
			value_column=value_column,
			id_type=id_type,
			value_type=self.type_str)

	@property
	def columns_values(self):
		"""Returns string with values of list columns that can be used in insertion"""
		res = ""
		for elem in self.name:
			if not isinstance(elem, str):
				res += ", " + str(elem)

		return res

	def _getListElements(self):
		"""Returns list of non-string name elements (i.e. corresponding to lists)"""
		return list(filter(lambda x: not isinstance(x, str), self.name))

	def pointsToListElement(self):
		"""Returns True if field points to element of the list"""
		return isinstance(self.name[-1], int)

	def getLastListColumn(self):
		"""Returns name and value of column corresponding to the last name element"""

		# This function makes sense only if self.pointsToListElement() is True
		if not self.pointsToListElement():
			raise interface.LogicError("Field should point to list element")

		list_elems = self._getListElements()
		col_num = len(list_elems) - 1 # index of last column
		col_name = self._getListColumnName(col_num)
		col_val = list_elems[col_num]
		return col_name, col_val

	@property
	def renumber_condition(self):
		"""Returns condition for renumbering after deletion of this element"""

		# This function makes sense only if self.pointsToListElement() is True
		if not self.pointsToListElement():
			raise interface.LogicError("Field should point to list element")

		self_copy = Field(self._engine, self.name)
		self_copy.name[-1] = None
		return self_copy.columns_condition

	@property
	def name_hashstr(self):
		"""
		Returns string that can serve as hash for field name along with its list elements
		"""
		name_copy = [repr(x) if x is not None else None for x in self.name]
		name_copy[-1] = None
		return self._engine.getSafeValue(self._engine.getNameString(name_copy))

	def __str__(self):
		return "Field (" + repr(self.name) + \
			(", value=" + repr(self.value) if self.value else "") + ")"

	def __repr__(self):
		return str(self)

	def __eq__(self, other):
		if other is None:
			return False

		if not isinstance(other, Field):
			return False

		return (self.name == other.name) and (self.value == other.value)


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

		def __init__(self, operand1, operator, operand2, invert=False):

			comparisons = [op.EQ, op.REGEXP, op.GT, op.GTE, op.LT, op.LTE]
			operators = [op.AND, op.OR]

			if operator in comparisons:
				# if node operator is a comparison, it is a leaf of condition tree
				val_class = operand2.__class__

				# check if value type is supported
				if operand2 is not None and val_class not in SUPPORTED_TYPES:
					raise FormatError("Operand type is not supported: " +
						val_class.__name__)

				# Nones only support EQ
				if operand2 is None and operator != op.EQ:
					raise FormatError("Null value can be only used in equality")

				# regexp is valid only for strings and blobs
				if operator == op.REGEXP and not val_class in [str, bytes]:
					raise FormatError("Values of type " + val_class.__name__ +
						" do not support regexp condition")
				self.leaf = True
			elif operator in operators:
				self.leaf = False
			else:
				raise FormatError("Wrong operator: " + str(operator))

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

		self.condition = copy.deepcopy(condition)

	def __str__(self):
		return "SearchRequest: " + str(self.condition)
