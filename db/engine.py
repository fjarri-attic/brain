import sqlite3
import re

from . import interface

class Sqlite3Engine(interface.Engine):

	__FIELD_SEP = '.'

	def __init__(self, path):
		self.__conn = sqlite3.connect(path)
		self.__conn.create_function("regexp", 2, self.__regexp)

	def dump(self):
		print("Dump:")
		for str in self.__conn.iterdump():
			print(str)
		print("--------")

	def __regexp(self, expr, item):
		r = re.compile(expr)
		return r.search(item) is not None

	def execute(self, sql_str, params=None):
		if params:
			return self.__conn.execute(sql_str, params)
		else:
			return self.__conn.execute(sql_str)

	def tableExists(self, name):
		res = list(self.__conn.execute("SELECT name FROM sqlite_master WHERE type='table'"))
		res = [x[0] for x in res]
		return name in res

	def tableIsEmpty(self, name):
		return len(list(self.__conn.execute("SELECT * FROM '" + name + "'"))) == 0

	def deleteTable(self, name):
		self.__conn.execute("DROP TABLE IF EXISTS '" + name + "'")

	def getEmptyCondition(self):
		return "SELECT 0 limit 0"

	def getSafeValueFromString(self, s):
		return "'" + s + "'"

	def getStringFromSafeValue(self, val):
		return val[1:-1]

	def getSafeTableNameFromList(self, l):
		temp_list = [(x if isinstance(x, str) else '') for x in l]
		return "_" + self.__FIELD_SEP.join(temp_list)

	def getListFromSafeTableName(self, name):
		return [(x if x != '' else None) for x in name[1:].split(self.__FIELD_SEP)]
