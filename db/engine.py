"""Module, containing wrappers for different DB engines"""

import sqlite3
import re

from . import interface

class Sqlite3Engine(interface.Engine):
	"""Wrapper for Sqlite 3 db engine"""

	__FIELD_SEP = '.' # separator for field elements in table name

	def __init__(self, path):

		# isolation_level=None disables autocommit, giving us the
		# possibility to manage transactions manually
		self.__conn = sqlite3.connect(path, isolation_level=None)

		# Add external regexp handling function
		self.__conn.create_function("regexp", 2, self.__regexp)

		self.cur = self.__conn.cursor()

	def dump(self):
		"""Dump the whole database to string; used for debug purposes"""
		print("Dump:")
		for str in self.__conn.iterdump():
			print(str)
		print("--------")

	def __regexp(self, expr, item):
		r = re.compile(expr)
		return r.search(item) is not None

	def execute(self, sql_str):
		return list(self.cur.execute(sql_str))

	def tableExists(self, name):
		res = list(self.cur.execute("SELECT * FROM sqlite_master WHERE type='table' AND name={name}"
			.format(name=self.getSafeValue(name))))
		return len(res) > 0

	def tableIsEmpty(self, name):
		return len(list(self.cur.execute("SELECT * FROM " + self.getSafeName(name)))) == 0

	def deleteTable(self, name):
		self.cur.execute("DROP TABLE IF EXISTS " + self.getSafeName(name))

	def getEmptyCondition(self):
		"""Returns condition for compound SELECT which evaluates to empty table"""
		return "SELECT 0 limit 0"

	def getSafeValue(self, s):
		"""Transform string value so that it could be safely used in queries"""
		return "'" + s.replace("'", "''") + "'"

	def getNameString(self, l):
		"""Get field name from list"""
		sep = self.__FIELD_SEP
		temp_list = [(x.replace('\\', '\\\\').replace(sep, '\\' + sep)
			if isinstance(x, str) else '') for x in l]
		return (sep + sep).join(temp_list)

	def getNameList(self, s):
		"""Get field name list from string"""
		sep = self.__FIELD_SEP
		l = s.split(sep + sep)
		return [(x.replace('\\' + sep, sep).replace('\\\\', '\\') if x != '' else None) for x in l]

	def getSafeName(self, s):
		"""Transform string value so that it could be safely used as table name"""
		return '"' + s.replace('"', '""') + '"'

	def begin(self):
		"""Begin transaction"""
		self.cur.execute("BEGIN TRANSACTION")
		pass

	def commit(self):
		"""Commit current transaction"""
		self.__conn.commit()

	def rollback(self):
		"""Rollback current transaction"""
		self.__conn.rollback()
