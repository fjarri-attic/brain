"""Unit tests for database enginr layer"""

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import unittest
from test import helpers
from db.engine import *

def getParameterized(base_class, name_prefix, db_class, db_file_name):
	"""Get named test suite with predefined setUp()"""
	class Derived(base_class):
		def setUp(self):
			self.engine = db_class(db_file_name)

	Derived.__name__ = name_prefix

	return Derived

class EngineTest(unittest.TestCase):
	"""Test for DB engines using base interface"""

	def testValueTransformation(self):
		"""Check that unsafe->safe value transformation works"""
		vals = ['a', 'b b', "a'b", "a''''b", "a'; DROP DATABASE;"]

		for val in vals:
			self.engine.begin()
			self.engine.execute("CREATE TABLE test (col TEXT)")
			self.engine.execute("INSERT INTO test VALUES ({value})".
				format(value=self.engine.getSafeValue(val)))
			res = list(self.engine.execute("SELECT col FROM test"))
			self.engine.execute("DROP TABLE test")
			self.engine.commit()

			self.failUnlessEqual(res, [(val,)])

	def testNameTransformation(self):
		"""Check that unsafe->safe table name transformation works"""
		names = ['a', 'b b' , 'a "" b', 'a"; DROP DATABASE;']

		for name in names:
			self.engine.begin()
			self.engine.execute("CREATE TABLE {table} (col TEXT)".format(table=self.engine.getSafeName(name)))
			self.engine.execute("INSERT INTO {table} VALUES ('aaa')".format(table=self.engine.getSafeName(name)))
			res = list(self.engine.execute("SELECT col FROM {table}".format(table=self.engine.getSafeName(name))))
			self.engine.execute("DROP TABLE {table}".format(table=self.engine.getSafeName(name)))
			self.engine.commit()

			self.failUnlessEqual(res, [('aaa',)])

	def testNameListToStr(self):
		"""Check that name list<->str transformation is a bijection (almost)"""
		names = [
			(['a.\\.b'], None),
			(['a\\.\\b', 'b\\\\..'], None),
			(['b\\.\\.\\', None, None], None),
			(['\\..\\\\.b', 1, 0], ['\\..\\\\.b', None, None])
		]

		for name, expected_res in names:
			if expected_res == None:
				expected_res = name

			name_str = self.engine.getNameString(name)
			name_list = self.engine.getNameList(name_str)
			self.failUnlessEqual(expected_res, name_list)

	def testExecute(self):
		"""Test execute() method on simple queries"""
		self.engine.begin()
		self.engine.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.engine.execute("INSERT INTO ttt VALUES ('a', 'b')")

	def testExecuteReturnsList(self):
		"""Test that execute() method returns list and not some specific class"""
		self.engine.begin()
		self.engine.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.engine.execute("INSERT INTO ttt VALUES ('a', 'b')")
		res = self.engine.execute("SELECT * FROM ttt")
		self.failUnless(isinstance(res, list))
		self.failUnlessEqual(res, [('a', 'b')])

	def testTableExists(self):
		"""Test work of tableExists() method for existing table"""
		self.engine.begin()
		self.engine.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.failUnless(self.engine.tableExists('ttt'))

	def testTableExistsMissingTable(self):
		"""Test work of tableExists() method for non-existing table"""
		self.engine.begin()
		self.engine.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.failIf(self.engine.tableExists('bbb'))

	def testTableIsEmpty(self):
		"""Test work of tableIsEmpty() method for empty table"""
		self.engine.begin()
		self.engine.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.failUnless(self.engine.tableIsEmpty('ttt'))

	def testTableIsEmptyFilledTable(self):
		"""Test work of tableIsEmpty() method for non-empty table"""
		self.engine.begin()
		self.engine.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.engine.execute("INSERT INTO ttt VALUES ('a', 'b')")
		self.failIf(self.engine.tableIsEmpty('ttt'))

	def testDeleteTable(self):
		"""Test work of deleteTable() method for existing table"""
		self.engine.begin()
		self.engine.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.engine.deleteTable('ttt')
		self.failIf(self.engine.tableExists('ttt'))

	def testDeleteTableMissingTable(self):
		"""Test work of deleteTable() method for non-existing table"""
		self.engine.begin()
		self.engine.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.engine.deleteTable('aaa')

	def testEmptyCondition(self):
		"""Test that empty condition really returns empty table"""
		self.engine.begin()
		res = self.engine.execute(self.engine.getEmptyCondition())
		self.failUnlessEqual(res, [])

	def testEmptyConditionIntersect(self):
		"""Test that empty condition can be used with INTERSECT"""
		self.engine.begin()
		self.engine.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.engine.execute("INSERT INTO ttt VALUES ('a', 'b')")
		res = self.engine.execute("SELECT * FROM (SELECT col1 FROM ttt) INTERSECT " +
			"SELECT * FROM (" + self.engine.getEmptyCondition() + ")")
		self.failUnlessEqual(res, [])

	def testEmptyConditionUnion(self):
		"""Test that empty condition can be used with UNION"""
		self.engine.begin()
		self.engine.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.engine.execute("INSERT INTO ttt VALUES ('a', 'b')")
		res = self.engine.execute("SELECT * FROM (SELECT col1 FROM ttt) UNION " +
			"SELECT * FROM (" + self.engine.getEmptyCondition() + ")")
		self.failUnlessEqual(res, [('a',)])

	def testRollbackTableModifications(self):
		"""Test that rollback rolls back table modifying actions"""
		self.engine.begin()
		self.engine.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.engine.execute("INSERT INTO ttt VALUES ('a', 'b')")
		self.engine.commit()

		self.engine.begin()
		self.engine.execute("INSERT INTO ttt VALUES ('c', 'd')")
		self.engine.execute("UPDATE ttt SET col1='e' WHERE col1='a'")
		self.engine.execute("DELETE FROM ttt WHERE col1='e'")
		self.engine.rollback()

		self.engine.begin()
		res = self.engine.execute("SELECT * FROM (SELECT col1 FROM ttt) UNION " +
			"SELECT * FROM (" + self.engine.getEmptyCondition() + ")")
		self.failUnlessEqual(res, [('a',)])

	def testRollbackTableCreation(self):
		"""Test that table creation can be rolled back"""
		self.engine.begin()
		self.engine.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.engine.rollback()

		self.failIf(self.engine.tableExists('ttt'))

	def testRollbackTableDeletion(self):
		"""Test that table deletion can be rolled back"""
		self.engine.begin()
		self.engine.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.engine.commit()

		self.engine.begin()
		self.engine.execute("DROP TABLE ttt")
		self.engine.rollback()

		self.failUnless(self.engine.tableExists('ttt'))

	def testRegexpSupport(self):
		"""Check that engine supports regexp search"""
		self.engine.begin()
		self.engine.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.engine.execute("INSERT INTO ttt VALUES ('abc', 'e')")
		self.engine.execute("INSERT INTO ttt VALUES ('bac', 'f')")
		self.engine.execute("INSERT INTO ttt VALUES ('cba', 'g')")
		res = self.engine.execute("SELECT col1 FROM ttt WHERE col1 REGEXP 'a\w+'")
		self.failUnlessEqual(res, [('abc',), ('bac',)])

	def testUnicodeSupport(self):
		"""Check that DB can store and return unicode values"""
		AUSTRIA = "\xd6sterreich"

		self.engine.begin()
		self.engine.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.engine.execute("INSERT INTO ttt VALUES ('abc', '{val}')"
			.format(val=AUSTRIA))

		# check that unicode string can be queried
		res = self.engine.execute("SELECT col2 FROM ttt")
		self.failUnlessEqual(res, [(AUSTRIA,)])

		# check that regexp works for unicode
		self.engine.execute("INSERT INTO ttt VALUES ('aaa', 'bbb')")
		res = self.engine.execute("SELECT col2 FROM ttt WHERE col2 REGEXP '{val}'"
			.format(val=AUSTRIA[:3]))
		self.failUnlessEqual(res, [(AUSTRIA,)])

def suite():
	"""Generate test suite for this module"""
	res = helpers.NamedTestSuite()

	parameters = [
		('memory.sqlite3', Sqlite3Engine, ':memory:'),
	]

	classes = [EngineTest]

	for parameter in parameters:
		for c in classes:
			res.addTest(unittest.TestLoader().loadTestsFromTestCase(
				getParameterized(c, *parameter)))

	return res
