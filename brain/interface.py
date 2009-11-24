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

	def __init__(self, engine, name, py_value=None, type_str=None, db_value=None):

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

			# some DB engines have case-insensitive table names, so we must
			# require lower case explicitly to avoid search errors
			if isinstance(elem, str) and not elem.islower():
				raise FormatError("Field name must be lowercase")

		# check value type
		if type(py_value) not in _SUPPORTED_TYPES:
			raise FormatError("Wrong value class: " + str(type(py_value)))

		self._engine = engine
		self._name = name[:]

		if py_value is not None:
			self.py_value = py_value
		elif type_str is not None:
			self.type_str = type_str
			if db_value is not None:
				self.db_value = db_value
		else:
			self.py_value = None

	@classmethod
	def fromNameStr(cls, engine, name_str, type_str=None):
		"""Create field object using stringified name"""

		# cut prefix 'field' from the resulting list
		return cls(engine, engine.getNameList(name_str)[1:], type_str=type_str)

	@classmethod
	def fromTableName(cls, engine, name_str):
		"""Create typed field object using field table name"""

		name_list = engine.getNameList(name_str)
		name = name_list[2:]
		type_str = name_list[1]

		obj = cls(engine, name, type_str=type_str)

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

	@property
	def name(self):
		return self._name

	def addNamePrefix(self, prefix):
		self._name = prefix + self._name

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
	def name_str(self):
		"""Returns name string with no type specifier"""
		return self._engine.getNameString(['field'] + self._name)

	@property
	def table_name(self):
		"""Returns field name in string form"""
		return self._engine.getNameString(['field', self.type_str] + self._name)

	@property
	def list_indexes_number(self):
		counter = 0
		for elem in self._name:
			if not isinstance(elem, str):
				counter += 1
		return counter

	@property
	def list_indexes_query(self):
		"""Returns string with list column names for this field"""
		numeric_columns = filter(lambda x: not isinstance(x, str), self._name)
		counter = 0
		l = []
		for column in numeric_columns:
			l.append(self._getListColumnName(counter))
			counter += 1

		# if value is null, this condition will be used alone,
		# so there's no need in leading comma
		return (', ' + ', '.join(l) if len(l) > 0 else '')

	@property
	def raw_list_indexes_condition(self):
		"""Returns string with condition for operations on given field"""

		# do not skip Nones, because we need them for
		# getting proper index of list column
		numeric_columns = filter(lambda x: not isinstance(x, str), self._name)
		counter = 0
		l = []
		for column in numeric_columns:
			if column is not None:
				l.append(self._getListColumnName(counter) +
					"=" + str(column))
			counter += 1

		return (' AND '.join(l) if len(l) > 0 else '')

	@property
	def list_indexes_condition(self):
		"""Returns string with condition for operations on given field, prefixed with AND"""

		cond = self.raw_list_indexes_condition
		return '' if cond == '' else (' AND ' + cond)

	def fillListIndexes(self, vals):
		"""Fill list indexes with given values"""
		reversed_vals = list(reversed(vals))
		for i, e in enumerate(self._name):
			if not isinstance(e, str):
				self._name[i] = reversed_vals.pop()

	def fillListIndexesFromField(self, field):
		"""Fill list indexes using other field as an example"""
		for i, e in enumerate(self._name):
			if i >= len(field.name):
				break

			if not isinstance(e, str):
				self._name[i] = field.name[i]

	def getCreationStr(self, id_column, value_column, id_type, list_index_type):
		"""Returns string containing list of columns necessary to create field table"""
		counter = 0
		res = ""
		for elem in self._name:
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
	def value_record(self):
		result = [self.db_value]
		for elem in self._name:
			if not isinstance(elem, str):
				result.append(elem)
		return result

	def _getListIndexes(self):
		"""Returns list of non-string name elements (i.e. corresponding to lists)"""
		return list(filter(lambda x: not isinstance(x, str), self._name))

	def pointsToListElement(self):
		"""Returns True if field points to element of the list"""
		return isinstance(self._name[-1], int)

	def getLastListColumn(self):
		"""
		Returns name and value of column corresponding to the last name element

		This function makes sense only if self.pointsToListElement() is True
		"""
		list_elems = self._getListIndexes()
		col_num = len(list_elems) - 1 # index of last column
		col_name = self._getListColumnName(col_num)
		col_val = list_elems[col_num]
		return col_name, col_val

	@property
	def renumber_condition(self):
		"""
		Returns condition for renumbering after deletion of this element

		This function makes sense only if self.pointsToListElement() is True
		"""
		self_copy = Field(self._engine, self._name)
		self_copy.name[-1] = None
		return self_copy.list_indexes_condition

	def matches(self, field):
		"""
		Returns True if this object can serve as a mask for given field
		(i.e. each element is either equal to other field's element with the same number,
		or is None when the corresponding element of other field is an integer)
		"""
		if len(field.name) > len(self._name):
			return False

		for i, e in enumerate(field.name):
			match = (e == self._name[i]) or \
				(self._name[i] is None and isinstance(e, int))

			if not match:
				return False

		return True

	def getAncestors(self):
		"""
		Returns list of field objects for each node in hierarchy, leading to this field
		(including self and root field, starting from root field).
		All resulting fields are untyped.
		"""
		name_copy = self._name[:]
		result = [Field(self._engine, name_copy)]
		while len(name_copy) > 0:
			name_copy.pop()
			result.append(Field(self._engine, name_copy))
		return list(reversed(result))

	def __str__(self):
		return "Field (" + repr(self._name) + \
			(", value=" + repr(self._value) if self._value is not None else "") + ")"

	def __repr__(self):
		return str(self)

	def __eq__(self, other):
		if other is None:
			return False

		if not isinstance(other, Field):
			return False

		return (self._name == other.name) and (self.py_value == other.py_value)


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

