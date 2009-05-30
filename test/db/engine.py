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
			self.db = db_class(db_file_name)

	Derived.__name__ = name_prefix

	return Derived

class EngineTest(unittest.TestCase):
	"""Test for DB engines using base interface"""

	def testValueTransformation(self):
		"""Check that safe<->unsafe value transformation is a bijection"""
		vals = ['a', 'b b']

		for val in vals:
			safe_val = self.db.getSafeValue(val)
			unsafe_val = self.db.getUnsafeValue(safe_val)
			self.failUnlessEqual(val, unsafe_val)

	def testTableNameTransformation(self):
		"""Check that safe<->unsafe table name transformation is a bijection (almost)"""
		names = [
			(['a'], None),
			(['a', 'b'], None),
			(['b', None, None], None),
			(['b', 1, 0], ['b', None, None])
		]

		for name, expected_res in names:
			if expected_res == None:
				expected_res = name

			name_str = self.db.getNameString(name)
			name_list = self.db.getNameList(name_str)
			self.failUnlessEqual(expected_res, name_list)

	def testExecuteWithoutParameters(self):
		"""Test that execute() method works with missing parameters"""
		self.db.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.db.execute("INSERT INTO ttt VALUES ('a', 'b')")

	def testExecuteWithParameters(self):
		"""Test that execute() method works with parameters list"""
		self.db.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.db.execute("INSERT INTO ttt VALUES (?, ?)", ('a', 'b'))

	def testExecuteReturnsList(self):
		"""Test that execute() method returns list and not some specific class"""
		self.db.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.db.execute("INSERT INTO ttt VALUES (?, ?)", ('a', 'b'))
		res = self.db.execute("SELECT * FROM ttt")
		self.failUnless(isinstance(res, list))
		self.failUnlessEqual(res, [('a', 'b')])

	def testTableExists(self):
		"""Test work of tableExists() method for existing table"""
		self.db.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.failUnless(self.db.tableExists('ttt'))

	def testTableExistsMissingTable(self):
		"""Test work of tableExists() method for non-existing table"""
		self.db.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.failIf(self.db.tableExists('bbb'))

	def testTableIsEmpty(self):
		"""Test work of tableIsEmpty() method for empty table"""
		self.db.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.failUnless(self.db.tableIsEmpty('ttt'))

	def testTableIsEmptyFilledTable(self):
		"""Test work of tableIsEmpty() method for non-empty table"""
		self.db.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.db.execute("INSERT INTO ttt VALUES ('a', 'b')")
		self.failIf(self.db.tableIsEmpty('ttt'))

	def testDeleteTable(self):
		"""Test work of deleteTable() method for existing table"""
		self.db.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.db.deleteTable('ttt')
		self.failIf(self.db.tableExists('ttt'))

	def testDeleteTableMissingTable(self):
		"""Test work of deleteTable() method for non-existing table"""
		self.db.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.db.deleteTable('aaa')

	def testEmptyCondition(self):
		"""Test that empty condition really returns empty table"""
		res = self.db.execute(self.db.getEmptyCondition())
		self.failUnlessEqual(res, [])

	def testEmptyConditionIntersect(self):
		"""Test that empty condition can be used with INTERSECT"""
		self.db.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.db.execute("INSERT INTO ttt VALUES ('a', 'b')")
		res = self.db.execute("SELECT * FROM (SELECT col1 FROM ttt) INTERSECT " +
			"SELECT * FROM (" + self.db.getEmptyCondition() + ")")
		self.failUnlessEqual(res, [])

	def testEmptyConditionUnion(self):
		"""Test that empty condition can be used with UNION"""
		self.db.execute("CREATE TABLE ttt (col1 TEXT, col2 TEXT)")
		self.db.execute("INSERT INTO ttt VALUES ('a', 'b')")
		res = self.db.execute("SELECT * FROM (SELECT col1 FROM ttt) UNION " +
			"SELECT * FROM (" + self.db.getEmptyCondition() + ")")
		self.failUnlessEqual(res, [('a',)])

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
