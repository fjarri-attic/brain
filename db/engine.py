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
		res = list(self.__conn.execute("SELECT name FROM sqlite_master WHERE type='table'"))
		res = [x[0] for x in res]
		return name in res

	def tableIsEmpty(self, name):
		return len(list(self.__conn.execute("SELECT * FROM '" + name + "'"))) == 0

	def deleteTable(self, name):
		self.__conn.execute("DROP TABLE IF EXISTS '" + name + "'")

	def getEmptyCondition(self):
		"""Returns condition for compound SELECT which evaluates to empty table"""
		return "SELECT 0 limit 0"

	def getSafeValueFromString(self, s):
		"""Transform string to something which can be safely used in query as a value"""
		return "'" + s + "'"

	def getStringFromSafeValue(self, val):
		"""Transform value back to original string"""
		return val[1:-1]

	def getSafeTableNameFromList(self, l):
		"""Transform list of strings to something which can be safely used as a part of table name"""
		temp_list = [(x if isinstance(x, str) else '') for x in l]
		return self.__FIELD_SEP.join(temp_list)

	def getListFromSafeTableName(self, name):
		"""Transform table name back to original list"""
		return [(x if x != '' else None) for x in name.split(self.__FIELD_SEP)]
