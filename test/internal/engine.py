"""Unit tests for database enginr layer"""

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import unittest
import helpers
from brain.engine import *


class EngineTest(unittest.TestCase):
	"""Test for DB engines using base interface"""

	def testValueTransformation(self):
		"""Check that unsafe->safe value transformation works"""
		vals = [
			'a', 'b b', "a'b", "a''''b", "a'; DROP DATABASE;", # strings
			0, 1, -1, 1234567890, # ints
			0.002, -0.0004, 5249512.3123, # floats
			b'\x00\x02\x03\x04', b'!@#$%^&*()' # binary buffers
		]

		table_name = 'test'

		for val in vals:
			self.engine.begin()
			self.engine.execute("CREATE TABLE {} (col {type})",
				[table_name], {'type': self.engine.getColumnType(val)})
			self.engine.execute("INSERT INTO {} VALUES (?)",
				[table_name], {}, [val])
			res = list(self.engine.execute("SELECT col FROM {}", [table_name]))
			self.engine.execute("DROP TABLE {}", [table_name])
			self.engine.commit()

			self.assertEqual(res, [(val,)])

	def testNameTransformation(self):
		"""Check that unsafe->safe table name transformation works"""
		names = ['a', 'b b' , 'a "" b', 'a"; DROP DATABASE;']

		test_val = 'aaa'
		val_type = self.engine.getColumnType(test_val)

		for name in names:
			self.engine.begin()
			self.engine.execute("CREATE TABLE {} (col {type})",
				[name], {'type': val_type})
			self.engine.execute("INSERT INTO {} VALUES (?)",
				[name], None, [test_val])
			res = list(self.engine.execute("SELECT col FROM {}", [name]))
			self.engine.execute("DROP TABLE {}", [name])
			self.engine.commit()

			self.assertEqual(res, [(test_val,)])

	def testNameListToStr(self):
		"""Check that name list<->str transformation is a bijection (almost)"""
		names = [
			(['a.\\.b'], None),
			(['a\\.\\b', 'b\\\\..'], None),
			(['b\\.\\.\\', None, None], None),
			(['\\..\\\\.b', 1, 0], ['\\..\\\\.b', None, None])
		]

		for name, expected_res in names:
			if expected_res is None:
				expected_res = name

			name_str = self.engine.getNameString(name)
			name_list = self.engine.getNameList(name_str)
			self.assertEqual(expected_res, name_list)

	def testExecute(self):
		"""Test execute() method on simple queries"""

		test_table = 'ttt'
		test_vals = ['a', 'b']
		val_type = self.engine.getColumnType(test_vals[0])

		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 {type}, col2 {type})",
			[test_table], {'type': val_type})
		self.engine.execute("INSERT INTO {} VALUES (?, ?)",
			[test_table], None, test_vals)

	def testExecuteReturnsList(self):
		"""Test that execute() method returns list and not some specific class"""

		test_table = 'ttt'
		test_vals = ['a', 'b']
		val_type = self.engine.getColumnType(test_vals[0])

		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 {type}, col2 {type})",
			[test_table], {'type': self.str_type})
		self.engine.execute("INSERT INTO {} VALUES (?, ?)",
			[test_table], None, test_vals)
		res = self.engine.execute("SELECT * FROM {}", [test_table])
		self.failUnless(isinstance(res, list))
		self.assertEqual(res, [tuple(test_vals)])

	def testTableExists(self):
		"""Test work of tableExists() method for existing table"""

		test_table = 'ttt'

		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 {type}, col2 {type})",
			[test_table], {'type': self.str_type})
		self.failUnless(self.engine.tableExists(test_table))

	def testTableExistsMissingTable(self):
		"""Test work of tableExists() method for non-existing table"""
		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 {type}, col2 {type})",
			['test'], {'type': self.str_type})
		self.assertFalse(self.engine.tableExists('bbb'))

	def testTableIsEmpty(self):
		"""Test work of tableIsEmpty() method for empty table"""
		table_name = 'ttt'
		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 {type}, col2 {type})",
			[table_name], {'type': self.str_type})
		self.failUnless(self.engine.tableIsEmpty(table_name))

	def testTableIsEmptyFilledTable(self):
		"""Test work of tableIsEmpty() method for non-empty table"""

		test_table = 'ttt'
		test_vals = ['a', 'b']
		val_type = self.engine.getColumnType(test_vals[0])

		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 {type}, col2 {type})",
			[test_table], {'type': self.str_type})
		self.engine.execute("INSERT INTO {} VALUES (?, ?)",
			[test_table], None, test_vals)
		self.assertFalse(self.engine.tableIsEmpty(test_table))

	def testDeleteTable(self):
		"""Test work of deleteTable() method for existing table"""
		test_table = 'ttt'
		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 {type}, col2 {type})",
			[test_table], {'type': self.str_type})
		self.engine.deleteTable(test_table)
		self.assertFalse(self.engine.tableExists(test_table))

	def testDeleteTableMissingTable(self):
		"""Test work of deleteTable() method for non-existing table"""
		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 {type}, col2 {type})",
			['ttt'], {'type': self.str_type})
		self.engine.deleteTable('aaa')

	def testRollbackTableModifications(self):
		"""Test that rollback rolls back table modifying actions"""

		test_table = 'ttt'

		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 {type}, col2 {type})",
			[test_table], {'type': self.str_type})
		self.engine.execute("INSERT INTO {} VALUES (?, ?)",
			[test_table], None, ['a', 'b'])
		self.engine.commit()

		self.engine.begin()
		self.engine.execute("INSERT INTO {} VALUES (?, ?)",
			[test_table], None, ['c', 'd'])
		self.engine.execute("UPDATE {} SET col1=? WHERE col1=?",
			[test_table], None, ['e', 'a'])
		self.engine.execute("DELETE FROM {} WHERE col1=?",
			[test_table], None, ['e'])
		self.engine.rollback()

		self.engine.begin()
		res = self.engine.execute("SELECT col1 FROM {}", [test_table])
		self.assertEqual(res, [('a',)])

	def testRollbackTableCreation(self):
		"""Test that table creation can be rolled back"""
		test_table = 'ttt'
		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 {type}, col2 {type})",
			[test_table], {'type': self.str_type})
		self.engine.rollback()

		self.assertFalse(self.engine.tableExists(test_table))

	def testRollbackTableDeletion(self):
		"""Test that table deletion can be rolled back"""
		test_table = 'ttt'
		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 {type}, col2 {type})",
			[test_table], {'type': self.str_type})
		self.engine.commit()

		self.engine.begin()
		self.engine.execute("DROP TABLE {}", [test_table])
		self.engine.rollback()

		self.failUnless(self.engine.tableExists(test_table))

	def testRegexpSupport(self):
		"""Check that engine supports regexp search"""
		test_table = 'ttt'
		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 {type}, col2 {type})",
			[test_table], {'type': self.str_type})
		self.engine.execute("INSERT INTO {} VALUES (?, ?)",
			[test_table], None, ['abc', 'e'])
		self.engine.execute("INSERT INTO {} VALUES (?, ?)",
			[test_table], None, ['bac', 'f'])
		self.engine.execute("INSERT INTO {} VALUES (?, ?)",
			[test_table], None, ['cba', 'g'])
		res = self.engine.execute("SELECT col1 FROM {} WHERE col1 {regexp} 'a\w+'",
			[test_table], {'regexp': self.engine.getRegexpOp()})
		self.assertEqual(res, [('abc',), ('bac',)])

	def testRegexpSupportInBlob(self):
		"""Check that engine supports regexp search in BLOB values"""
		test_table = 'ttt'
		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 {type})",
			[test_table], {'type': self.engine.getColumnType(bytes())})
		for val in [b'\x00\x01\x02', b'\x01\x00\x02', b'\x01\x02\x00']:
			self.engine.execute("INSERT INTO {} VALUES (?)",
				[test_table], None, [val])
		res = self.engine.execute("SELECT col1 FROM {} WHERE col1 {regexp} ?",
			[test_table], {'regexp': self.engine.getRegexpOp()}, [b'\x00.+'])
		self.assertEqual(res, [(b'\x00\x01\x02',), (b'\x01\x00\x02',)])

	def testUnicodeSupport(self):
		"""Check that DB can store and return unicode values"""
		austria = "\xd6sterreich"
		test_table = 'ttt'

		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 {type}, col2 {type})",
			[test_table], {'type': self.str_type})
		self.engine.execute("INSERT INTO {} VALUES (?, ?)",
			[test_table], None, ['abc', austria])

		# check that unicode string can be queried
		res = self.engine.execute("SELECT col2 FROM {}", [test_table])
		self.assertEqual(res, [(austria,)])

		# check that regexp works for unicode
		self.engine.execute("INSERT INTO {} VALUES (?, ?)",
			[test_table], None, ['aaa', 'bbb'])
		res = self.engine.execute("SELECT col2 FROM {} WHERE col2 {regexp} ?",
			[test_table], {'regexp': self.engine.getRegexpOp()}, [austria[:3]])
		self.assertEqual(res, [(austria,)])

	def testTypeMapping(self):
		"""Check that necessary Python types can be mapped to SQL types and back"""
		for cls in [str, int, float, bytes]:
			sql_type = self.engine.getColumnType(cls())
			py_cls = self.engine.getValueClass(sql_type)
			self.assertEqual(py_cls, cls)

	def testNullValue(self):
		"""Check that NULLs and operations with them are supported"""
		test_table = 'ttt'
		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 {type}, col2 {type})",
			[test_table], {'type': self.str_type})
		self.engine.execute("INSERT INTO {} VALUES (?, ?)",
			[test_table], None, ['a', 'b'])
		self.engine.execute("INSERT INTO {} VALUES (?, ?)",
			[test_table], None, ['a', None])
		self.engine.execute("INSERT INTO {} VALUES (?, ?)",
			[test_table], None, [None, 'b'])
		res = self.engine.execute("SELECT col2 FROM {} WHERE col1 ISNULL", [test_table])
		self.engine.commit()

		self.assertEqual(res, [('b',)])

	def testIdCounter(self):
		"""Simple check for internal ID counter"""
		id1 = self.engine.getNewId()
		id2 = self.engine.getNewId()
		self.assertNotEqual(id1, id2)

	def testIdCounterType(self):
		"""Check that ID has proper type"""
		id1 = self.engine.getNewId()

		self.assertEqual(
			self.engine.getIdType(),
			self.engine.getColumnType(id1))


def suite(name, engine_tag, path, open_existing):

	class Derived(EngineTest):
		def setUp(self):
			self.engine = getEngineByTag(engine_tag)(path, open_existing)
			self.str_type = self.engine.getColumnType(str())

		def tearDown(self):
			self.engine.close()

	Derived.__name__ = name
	res = helpers.NamedTestSuite()
	res.addTest(unittest.TestLoader().loadTestsFromTestCase(Derived))
	return res
