"""Module, containing wrappers for different DB engines"""

import sqlite3

postgresql_available = False
try:
	import postgresql
	postgresql_available = True
except:
	pass

import re
import os
import os.path

from . import interface

def getEngineTags():
	return list(_DB_ENGINES.keys())

def getDefaultEngineTag():
	return getEngineTags()[0]

def getEngineByTag(tag):
	if tag is None:
		return _DB_ENGINES[getDefaultEngineTag()]
	else:
		return _DB_ENGINES[tag]


class _Engine:
	"""Engine layer class interface"""

	def close(self):
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


class _Sqlite3Engine(_Engine):
	"""Wrapper for Sqlite 3 db engine"""

	__FIELD_SEP = '.' # separator for field elements in table name

	def __init__(self, name=None, open_existing=None):

		if name is None:
			name = ':memory:'
		else:
			if open_existing == 1:
			# do not create DB if it does not exist
				if not os.path.exists(name):
					raise Exception(name + " was not found")
			elif open_existing == 0:
			# recreate DB even if such file already exists
				if os.path.exists(name):
					os.remove(name)

		# isolation_level=None disables autocommit, giving us the
		# possibility to manage transactions manually
		self._conn = sqlite3.connect(name, isolation_level=None)

		# Add external regexp handling function
		self._conn.create_function("regexp", 2, self.__regexp)

		self._cur = self._conn.cursor()

	def close(self):
		self._conn.close()

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
		for str in self._conn.iterdump():
			print(str)
		print("--------")

	def __regexp(self, expr, item):
		"""Callback for regexp condition support in DB"""
		r = re.compile(expr)
		return r.search(item) is not None

	def execute(self, sql_str, tables=None, values=None):
		"""Execute given SQL query"""
		if tables is not None:
			tables = [self.getSafeName(x) for x in tables]
			tables_tuple = tuple(tables)
			sql_str = sql_str.format(*tables_tuple)
		if values is None:
			cur = self._cur.execute(sql_str)
		else:
			cur = self._cur.execute(sql_str, tuple(values))
		return cur.fetchall()

	def tableExists(self, name):
		res = list(self._cur.execute("SELECT * FROM sqlite_master WHERE type='table' AND name=?",
			(name,)))
		return len(res) > 0

	def tableIsEmpty(self, name):
		return list(self._cur.execute("SELECT COUNT(*) FROM " + self.getSafeName(name)))[0][0] == 0

	def deleteTable(self, name):
		self._cur.execute("DROP TABLE IF EXISTS " + self.getSafeName(name))

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
		self._cur.execute("BEGIN TRANSACTION")

	def commit(self):
		"""Commit current transaction"""
		self._conn.commit()

	def rollback(self):
		"""Rollback current transaction"""
		self._conn.rollback()

	def getRegexpOp(self):
		return "REGEXP"


class _PostgreEngine(_Engine):
	"""Wrapper for PostgreSQL db engine"""

	__FIELD_SEP = '.' # separator for field elements in table name

	def __init__(self, name=None, open_existing=None, host='localhost',
		port=5432, user='postgres', password='1q2w3e'):

		if name is None:
			name = 'temp'
			open_existing = 0

		conn = postgresql.open(user=user,
			password=password, host=host, port=port)

		query = conn.prepare("SELECT datname FROM pg_catalog.pg_database")
		db_list = [x[0] for x in query()]
		db_exists = name in db_list

		if open_existing == 1:
		# do not create DB if it does not exist
			if not db_exists:
				raise Exception(name + " was not found")
		elif open_existing == 0:
		# recreate DB even if such file already exists
			if db_exists:
				conn.execute("DROP DATABASE " + self.getSafeName(name))
			conn.execute("CREATE DATABASE " + self.getSafeName(name))

		conn.close()

		self._conn = postgresql.open(user=user,
			password=password, host=host, port=port, database=name)

		self._transaction = None

	def close(self):
		self._conn.close()

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
		for str in self._conn.iterdump():
			print(str)
		print("--------")

	def execute(self, sql_str, tables, values):
		"""Execute given SQL query"""
		if tables is not None:
			tables = [self.getSafeName(x) for x in tables]
			tables_tuple = tuple(tables)
			sql_str = sql_str.format(*tables_tuple)
		if values is None: values = []
		values = tuple(values)
		return self._conn.prepare(sql_str)(*values)

	def tableExists(self, name):
		res = self._conn.prepare("SELECT * FROM pg_tables WHERE tablename=?")(name)
		return len(res) > 0

	def tableIsEmpty(self, name):
		return self._conn.prepare("SELECT COUNT(*) FROM " + self.getSafeName(name))()[0][0] == 0

	def deleteTable(self, name):
		if self.tableExists(name):
			self._conn.prepare("DROP TABLE " + self.getSafeName(name))()

	def getColumnType(self, val):
		"""Return SQL type for storing given value"""
		types = {
			str: "TEXT", int: "INT8", float: "FLOAT8", bytes: "BYTEA"
		}
		return types[val.__class__]

	def getValueClass(self, type_str):
		"""Return Python class for the given SQL type"""
		classes = {
			"TEXT": str, "INT8": int, "FLOAT8": float, "BYTEA": bytes
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
		self._transaction = self._conn.xact()
		self._transaction.start()

	def commit(self):
		"""Commit current transaction"""
		try:
			self._transaction.commit()
		finally:
			self._transaction = None

	def rollback(self):
		"""Rollback current transaction"""
		try:
			self._transaction.rollback()
		finally:
			self._transaction = None

	def getRegexpOp(self):
		return "~"


_DB_ENGINES = {
	'sqlite3': _Sqlite3Engine
}

if postgresql_available:
	_DB_ENGINES['postgre'] = _PostgreEngine
