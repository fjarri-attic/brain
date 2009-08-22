"""Internal interface classes"""

from . import op

_POINTER_TYPES = [type(None), list, dict]
_SUPPORTED_TYPES = [int, str, float, bytes] + _POINTER_TYPES


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

class EngineError(BrainError):
	"""Signals an error in DB engine wrapper"""
	pass

#
# Classes
#

class Pointer:
	"""Class, representing special type of DB values - Nones and pointers to structures"""

	def __init__(self):
		self._py_to_db = {type(None): 0, dict: 1, list: 2}
		self._db_to_py = {self._py_to_db[key]: key for key in self._py_to_db}
		self.py_value = None

	@classmethod
	def fromPyValue(cls, py_value):
		obj = cls()
		obj.py_value = py_value
		return obj

	# Property, representing value in Python form

	def __get_py_value(self):
		return self._py_value

	def __set_py_value(self, py_value):
		if type(py_value) not in self._py_to_db:
			raise FormatError("Not supported Python value: " + repr(py_value))

		self._db_value = self._py_to_db[type(py_value)]
		self._py_value = py_value

	py_value = property(__get_py_value, __set_py_value)

	# Property, representing value in DB form

	def __get_db_value(self):
		return self._db_value

	def __set_db_value(self, db_value):
		self._py_value = None if db_value == self._py_to_db[type(None)] else \
			self._db_to_py[db_value]()
		self._db_value = db_value

	db_value = property(__get_db_value, __set_db_value)

	def __str__(self):
		if isinstance(self._py_value, dict):
			return "Pointer to dict"
		elif isinstance(self._py_value, list):
			return "Pointer to list"
		else:
			return "Pointer to None"

	def __repr__(self):
		return str(self)


class Field:
	"""Class for more convenient handling of Field objects"""

	def __init__(self, engine, name, value=None):

		if not isinstance(name, list):
			raise FormatError("Field name should be list")

		# check that list contains only strings, ints and Nones
		for elem in name:
			if type(elem) not in [type(None), str, int]:
				raise FormatError("Field name list must contain only integers, strings or Nones")

			# name element should not be an empty string so that
			# it is not confused with list element
			if elem == '':
				raise FormatError("Field name element should not be an empty string")

		# check value type
		if type(value) not in _SUPPORTED_TYPES:
			raise FormatError("Wrong value class: " + str(type(value)))

		self._engine = engine
		self.name = name[:]
		self.py_value = value

	@classmethod
	def fromNameStrNoType(cls, engine, name_str, value=None):
		"""Create object using stringified name without embedded type instead of list"""

		# cut prefix 'field' from the resulting list
		return cls(engine, engine.getNameList(name_str)[1:], value)

	@classmethod
	def fromNameStr(cls, engine, name_str):
		"""Create object using stringified name instead of list"""

		name_list = engine.getNameList(name_str)
		name = name_list[2:]
		type_str = name_list[1]

		obj = cls(engine, name)
		obj.type_str = type_str

		return obj

	@classmethod
	def isFieldTableName(cls, engine, name_str):
		"""Returns True if given name resembles name of the field table"""

		# FIXME: this method is not very reliable, because it implies that
		# engine creates name strings in format "<elem><sep><elem><sep>..."
		return name_str.startswith(engine.getNameString(['field']))

	def _getListColumnName(self, index):
		"""Get name of additional list column corresponding to given index"""
		return "c" + str(index)

	# Property, containing field value type

	def __get_type_str(self):
		"""Returns string with SQL type for stored value"""
		return self._engine.getColumnType(self._value)

	def __set_type_str(self, type_str):
		"""Set field type using given value from specification table"""
		self._value = self._engine.getValueClass(type_str)()

	type_str = property(__get_type_str, __set_type_str)

	# Property, containing value in DB representation

	def __get_db_value(self):
		if isinstance(self._value, Pointer):
			return self._value.db_value
		else:
			return self._value

	def __set_db_value(self, db_value):
		if isinstance(self._value, Pointer):
			self._value.db_value = db_value
		else:
			self._value = db_value

	db_value = property(__get_db_value, __set_db_value)

	# Property, containing value in Python representation

	def __get_py_value(self):
		if isinstance(self._value, Pointer):
			return self._value.py_value
		else:
			return self._value

	def __set_py_value(self, py_value):
		if type(py_value) in _POINTER_TYPES:
			self._value = Pointer.fromPyValue(py_value)
		else:
			self._value = py_value

	py_value = property(__get_py_value, __set_py_value)

	@property
	def name_str_no_type(self):
		"""Returns name string with no type specifier"""
		return self._engine.getNameString(['field'] + self.name)

	@property
	def name_str(self):
		"""Returns field name in string form"""
		return self._engine.getNameString(['field', self.type_str] + self.name)

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
		return (', ' + ', '.join(l) if len(l) > 0 else '')

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
			", {value_column} {value_type}" + res).format(
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
		"""
		Returns name and value of column corresponding to the last name element

		This function makes sense only if self.pointsToList() is True
		"""
		list_elems = self._getListElements()
		col_num = len(list_elems) - 1 # index of last column
		col_name = self._getListColumnName(col_num)
		col_val = list_elems[col_num]
		return col_name, col_val

	@property
	def renumber_condition(self):
		"""
		Returns condition for renumbering after deletion of this element

		This function makes sense only if self.pointsToList() is True
		"""
		self_copy = Field(self._engine, self.name)
		self_copy.name[-1] = None
		return self_copy.columns_condition

	def __str__(self):
		return "Field (" + repr(self.name) + \
			(", value=" + repr(self._value) if self._value is not None else "") + ")"

	def __repr__(self):
		return str(self)

	def __eq__(self, other):
		if other is None:
			return False

		if not isinstance(other, Field):
			return False

		return (self.name == other.name) and (self.py_value == other.py_value)


#
# Requests
#

class CreateRequest:
	"""Request for object creation"""

	def __init__(self, fields):

		if fields is None or fields == []:
			raise FormatError("Cannot create empty object")

		self.fields = fields

	def __str__(self):
		return type(self).__name__ + " for fields list " + repr(self.fields)


class ModifyRequest:
	"""Request for modification of existing objects"""

	def __init__(self, id, path, fields, remove_conflicts):

		if id is None:
			raise FormatError("Cannot modify undefined object")

		self.id = id
		self.path = path
		self.fields = fields
		self.remove_conflicts = remove_conflicts

	def __str__(self):
		return "{name} for object {id}{data}{remove_conflicts}".format(
			name=type(self).__name__,
			id=self.id,
			data=("" if self.fields is None else ": " + repr(self.fields)),
			remove_conflicts=", remove conflicts" if self.remove_conflicts else "")


class DeleteRequest:
	"""Request for deletion of existing object or its fields"""
	def __init__(self, id, fields=None):

		if id is None:
			raise FormatError("Cannot delete undefined object")

		self.id = id
		self.fields = fields

	def __str__(self):
		return "{name} for object {id}{data}".format(
			name=type(self).__name__,
			id=self.id,
			data=("" if self.fields is None else ": " + repr(self.fields)))


class ReadRequest:
	"""Request for reading existing object or its fields"""
	def __init__(self, id, path=None, masks=None):

		if id is None:
			raise FormatError("Cannot read undefined object")

		# path should be determined
		if path is not None:
			for elem in path.name:
				if elem is None:
					raise FormatError("Path should not have None parts in name")

		self.id = id
		self.path = path
		self.masks = masks

	def __str__(self):
		return "{name} for object {id}{path}{masks}".format(
			name=type(self).__name__,
			id=self.id,
			path="" if self.path is None else (", path: " + repr(self.path)),
			masks="" if self.masks is None else ", masks: " + repr(self.masks))


class InsertRequest:
	"""Request for insertion into list of fields"""

	def __init__(self, id, path, field_groups, remove_conflicts):

		# path should be determined, except maybe for the last element
		for elem in path.name[:-1]:
			if elem is None:
				raise FormatError("Target field should not have None parts in name, " +
					"except for the last one")

		# target field should point on list
		if path.name[-1] is not None and not isinstance(path.name[-1], int):
			raise FormatError("Last element of target field name should be None or integer")

		# all fields to insert should be fully determined
		for field_group in field_groups:
			for field in field_group:
				for elem in field.name:
					if elem is None:
						raise FormatError("Each of fields to insert should be determined")

		if id is None:
			raise FormatError("Cannot modify undefined object")

		# Initialize fields
		self.id = id
		self.field_groups = field_groups
		self.path = path
		self.remove_conflicts = remove_conflicts

	def __str__(self):
		return "{name} for object {id} and path {path}: {data}{remove_conflicts}".format(
			name=type(self).__name__,
			id=self.id,
			path=self.path,
			data=repr(self.field_groups),
			remove_conflicts=", remove conflicts" if self.remove_conflicts else "")


class SearchRequest:
	"""Request for searching in database"""

	class Condition:
		"""Class for main element of search request"""

		def __init__(self, operand1, operator, operand2, invert=False):

			comparisons = [op.EQ, op.REGEXP, op.GT, op.GTE, op.LT, op.LTE]
			operators = [op.AND, op.OR]

			if operator in comparisons:

				# if node operator is a comparison, it is a leaf of condition tree
				val = operand2.py_value

				# Nones only support EQ
				if val is None and operator != op.EQ:
					raise FormatError("Null value can be only used in equality")

				# regexp is valid only for strings and blobs
				if operator == op.REGEXP and type(val) not in [str, bytes]:
					raise FormatError("Values of type " + type(val).__name__ +
						" do not support regexp condition")
				self.leaf = True
			elif operator in operators:
				self.leaf = False

				if not isinstance(operand1, SearchRequest.Condition) or \
					not isinstance(operand2, SearchRequest.Condition):
					raise FormatError("Both operands should be conditions")
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

	def __init__(self, condition=None):
		self.condition = condition

	def __str__(self):
		return "SearchRequest: " + str(self.condition)


class ObjectExistsRequest:
	"""Request for searching for object in database"""

	def __init__(self, id):
		if id is None:
			raise FormatError("Cannot modify undefined object")

		self.id = id

	def __str__(self):
		return "{name} for object {id}".format(
			name=type(self).__name__,
			id=self.id)


class DumpRequest:
	"""Request for dumping database contents"""

	def __str__(self):
		return type(self).__name__


class RepairRequest:
	"""Request for rebuilding caching tables"""

	def __str__(self):
		return type(self).__name__

