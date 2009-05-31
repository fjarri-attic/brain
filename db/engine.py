"""Module, containing wrappers for different DB engines"""

import sqlite3
import re

from . import interface

class Sqlite3Engine(interface.Engine):
	"""Wrapper for Sqlite 3 db engine"""

	__FIELD_SEP = '.' # separator for field elements in table name

	def __init__(self, path):
		self.__conn = sqlite3.connect(path)
		self.__conn.create_function("regexp", 2, self.__regexp)

	def dump(self):
		"""Dump the whole database to string; used for debug purposes"""
		print("Dump:")
		for str in self.__conn.iterdump():
			print(str)
		print("--------")

	def __regexp(self, expr, item):
		r = re.compile(expr)
		return r.search(item) is not None

	def execute(self, sql_str, params=None):
		if params:
			return list(self.__conn.execute(sql_str, params))
		else:
			return list(self.__conn.execute(sql_str))

	def tableExists(self, name):
		# FIXME: we should not depend on "safe table name" format here
		if name[0] == '"':
			safe_name = name[1:-1]
		else:
			safe_name = name
		res = list(self.__conn.execute("SELECT * FROM sqlite_master WHERE type='table' AND name='{name}'"
			.format(name=safe_name)))
		return len(res) > 0

	def tableIsEmpty(self, name):
		return len(list(self.__conn.execute("SELECT * FROM " + name))) == 0

	def deleteTable(self, name):
		self.__conn.execute("DROP TABLE IF EXISTS '" + name + "'")

	def getEmptyCondition(self):
		"""Returns condition for compound SELECT which evaluates to empty table"""
		return "SELECT 0 limit 0"

	def getSafeValue(self, s):
		"""
		Transform string to something which can be safely used as a part of a value
		Be sure to keep the property: f^-1(f(a) + f(b)) = a + b
		"""
		return s

	def getUnsafeValue(self, s):
		"""
		Transform part of safe value to unsafe original
		Be sure to keep the property: f^-1(f(a) + f(b)) = a + b
		"""
		return s
		
	def getSafeRegexp(self, s):
		"""Transform given regexp so that it can be used as a part of a query"""
		return "'" + s + "'"

	def getQuotedSafeValue(self, s):
		"""Quote safe value so that it can be used as a part of a query"""
		return "'" + s + "'"

	def getNameString(self, l):
		"""Get field name from list"""
		temp_list = [(x if isinstance(x, str) else '') for x in l]
		return self.__FIELD_SEP.join(temp_list)

	def getNameList(self, s):
		"""Get field name list from string"""
		return [(x if x != '' else None) for x in s.split(self.__FIELD_SEP)]

	def getSafeName(self, s):
		return s

	def getQuotedSafeName(self, s):
		return '"' + s + '"'
