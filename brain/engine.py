"""Module, containing wrappers for different DB engines"""

import sqlite3
import re
import os
import os.path

from . import interface


class Engine:
	"""Engine layer class interface"""

	def disconnect(self):
		raise NotImplementedError

	def dump(self):
		"""Dump the whole database to string; used for debug purposes"""
		raise NotImplementedError

	def execute(self, sql_str):
		"""Execute given SQL query"""
		raise NotImplementedError

	def getNewId(self):
		"""Return new unique ID for this database"""
		raise NotImplementedError

	def getIdType(self):
		"""Return type string for IDs used in this database"""
		raise NotImplementedError

	def tableExists(self, name): raise NotImplementedError
	def tableIsEmpty(self, name): raise NotImplementedError
	def deleteTable(self, name): raise NotImplementedError

	def getEmptyCondition(self):
		"""Returns condition for compound SELECT which evaluates to empty table"""
		raise NotImplementedError

	def getSafeValue(self, s):
		"""Transform string value so that it could be safely used in queries"""
		raise NotImplementedError

	def getColumnType(self, val):
		"""Return SQL type for storing given value"""
		raise NotImplementedError

	def getValueClass(self, type_str):
		"""Return Python class for the given SQL type"""
		raise NotImplementedError

	def getNameString(self, l):
		"""Get field name from list"""
		raise NotImplementedError

	def getNameList(self, s):
		"""Get field name list from string"""
		raise NotImplementedError

	def getSafeName(self, s):
		"""Transform string value so that it could be safely used as table name"""
		raise NotImplementedError

	def getNullValue(self):
		"""Returns null value to use in queries"""
		raise NotImplementedError

	def begin(self):
		"""Begin transaction"""
		raise NotImplementedError

	def commit(self):
		"""Commit current transaction"""
		raise NotImplementedError

	def rollback(self):
		"""Rollback current transaction"""
		raise NotImplementedError


class Sqlite3Engine(Engine):
	"""Wrapper for Sqlite 3 db engine"""

	__FIELD_SEP = '.' # separator for field elements in table name

	def __init__(self, path=None, open_existing=None):

		if path is None:
			path = ':memory:'
		else:
			if open_existing == 1:
			# do not create DB if it does not exist
				if not os.path.exists(path):
					raise Exception(path + " was not found")
			elif open_existing == 0:
			# recreate DB even if such file already exists
				if os.path.exists(path):
					os.remove(path)

		# isolation_level=None disables autocommit, giving us the
		# possibility to manage transactions manually
		self.__conn = sqlite3.connect(path, isolation_level=None)

		# Add external regexp handling function
		self.__conn.create_function("regexp", 2, self.__regexp)

		self.cur = self.__conn.cursor()

	def disconnect(self):
		self.__conn.close()

	def getNewId(self):
		if not self.tableExists('max_uuid'):
			self.execute("CREATE TABLE max_uuid (uuid {type})".format(
				type=self.getIdType()))
			self.execute("INSERT INTO max_uuid VALUES (0)")

		self.execute("UPDATE max_uuid SET uuid=uuid+1")
		res = self.execute("SELECT uuid FROM max_uuid")

		return res[0][0]

	def getIdType(self):
		return self.getColumnType(int())

	def dump(self):
		"""Dump the whole database to string; used for debug purposes"""
		print("Dump:")
		for str in self.__conn.iterdump():
			print(str)
		print("--------")

	def __regexp(self, expr, item):
		"""Callback for regexp condition support in DB"""
		r = re.compile(expr)
		return r.search(item) is not None

	def execute(self, sql_str):
		"""Execute given SQL query"""
		cur = self.cur.execute(sql_str)
		return cur.fetchall()

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

	def getSafeValue(self, val):
		"""Transform value so that it could be safely used in queries"""
		transformations = {
			str: lambda x: "'" + x.replace("'", "''") + "'",
			int: lambda x: str(x),
			float: lambda x: str(x),
			# FIXME: seems that now .decode() is broken, so we have to do this ugly thing
			bytes: lambda x: "X'" + ''.join(["{0:02X}".format(x) for x in val]) + "'"
		}
		return transformations[val.__class__](val)

	def getColumnType(self, val):
		"""Return SQL type for storing given value"""
		types = {
			str: "TEXT", int: "INTEGER", float: "FLOAT", bytes: "BLOB"
		}
		return types[val.__class__]

	def getValueClass(self, type_str):
		"""Return Python class for the given SQL type"""
		classes = {
			"TEXT": str, "INTEGER": int, "FLOAT": float, "BLOB": bytes
		}
		return classes[type_str]

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

	def getNullValue(self):
		return 'NULL'

	def begin(self):
		"""Begin transaction"""
		self.cur.execute("BEGIN TRANSACTION")

	def commit(self):
		"""Commit current transaction"""
		self.__conn.commit()

	def rollback(self):
		"""Rollback current transaction"""
		self.__conn.rollback()
