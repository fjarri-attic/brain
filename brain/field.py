import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import brain

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
