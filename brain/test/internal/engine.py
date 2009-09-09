"""Unit tests for database enginr layer"""

import unittest

import helpers

import brain
from brain.engine import *
from brain.interface import Field


class EngineTest(helpers.NamedTestCase):
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
			val_type = self.engine.getColumnType(val)

			self.engine.begin()
			self.engine.execute("CREATE TABLE {} (col " + val_type + ")", [table_name])
			self.engine.execute("INSERT INTO {} VALUES (?)", [table_name], [val])
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
			self.engine.execute("CREATE TABLE {} (col " + val_type + ")", [name])
			self.engine.execute("INSERT INTO {} VALUES (?)", [name], [test_val])
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
		self.engine.execute("CREATE TABLE {} (col1 " + val_type + ", col2 " + val_type + ")",
			[test_table])
		self.engine.execute("INSERT INTO {} VALUES (?, ?)", [test_table], test_vals)

	def testExecuteReturnsList(self):
		"""Test that execute() method returns list and not some specific class"""

		test_table = 'ttt'
		test_vals = ['a', 'b']
		val_type = self.engine.getColumnType(test_vals[0])

		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 " + val_type + ", col2 " + val_type + ")",
			[test_table])
		self.engine.execute("INSERT INTO {} VALUES (?, ?)", [test_table], test_vals)
		res = self.engine.execute("SELECT * FROM {}", [test_table])
		self.failUnless(isinstance(res, list))
		self.assertEqual(res, [tuple(test_vals)])

	def testTableExists(self):
		"""Test work of tableExists() method for existing table"""
		test_table = 'ttt'
		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 " + self.str_type + ")", [test_table])
		self.failUnless(self.engine.tableExists(test_table))

	def testTableExistsMissingTable(self):
		"""Test work of tableExists() method for non-existing table"""
		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 " + self.str_type + ")", ['test'])
		self.assertFalse(self.engine.tableExists('bbb'))

	def testTableIsEmpty(self):
		"""Test work of tableIsEmpty() method for empty table"""
		table_name = 'ttt'
		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 " + self.str_type + ")", [table_name])
		self.failUnless(self.engine.tableIsEmpty(table_name))

	def testTableIsEmptyFilledTable(self):
		"""Test work of tableIsEmpty() method for non-empty table"""

		test_table = 'ttt'
		test_vals = ['a', 'b']
		val_type = self.engine.getColumnType(test_vals[0])

		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 " + val_type + ", col2 " + val_type + ")",
			[test_table])
		self.engine.execute("INSERT INTO {} VALUES (?, ?)", [test_table], test_vals)
		self.assertFalse(self.engine.tableIsEmpty(test_table))

	def testDeleteTable(self):
		"""Test work of deleteTable() method for existing table"""
		test_table = 'ttt'
		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 " + self.str_type + ")", [test_table])
		self.engine.deleteTable(test_table)
		self.assertFalse(self.engine.tableExists(test_table))

	def testDeleteTableMissingTable(self):
		"""Test work of deleteTable() method for non-existing table"""
		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 " + self.str_type + ")", ['ttt'])
		self.engine.deleteTable('aaa')

	def testRollbackTableModifications(self):
		"""Test that rollback rolls back table modifying actions"""

		test_table = 'ttt'

		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 " + self.str_type +
			", col2 " + self.str_type + ")", [test_table])
		self.engine.execute("INSERT INTO {} VALUES (?, ?)",
			[test_table], ['a', 'b'])
		self.engine.commit()

		self.engine.begin()
		self.engine.execute("INSERT INTO {} VALUES (?, ?)", [test_table], ['c', 'd'])
		self.engine.execute("UPDATE {} SET col1=? WHERE col1=?", [test_table], ['e', 'a'])
		self.engine.execute("DELETE FROM {} WHERE col1=?", [test_table], ['e'])
		self.engine.rollback()

		self.engine.begin()
		res = self.engine.execute("SELECT col1 FROM {}", [test_table])
		self.assertEqual(res, [('a',)])

	def testRollbackTableCreation(self):
		"""Test that table creation can be rolled back"""
		test_table = 'ttt'
		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 " + self.str_type + ")", [test_table])
		self.engine.rollback()

		self.assertFalse(self.engine.tableExists(test_table))

	def testRollbackTableDeletion(self):
		"""Test that table deletion can be rolled back"""
		test_table = 'ttt'
		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 " + self.str_type + ")", [test_table])
		self.engine.commit()

		self.engine.begin()
		self.engine.execute("DROP TABLE {}", [test_table])
		self.engine.rollback()

		self.failUnless(self.engine.tableExists(test_table))

	def testRegexpSupport(self):
		"""Check that engine supports regexp search"""
		test_table = 'ttt'
		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 " + self.str_type + ")", [test_table])
		self.engine.execute("INSERT INTO {} VALUES (?)", [test_table], ['abc'])
		self.engine.execute("INSERT INTO {} VALUES (?)", [test_table], ['bac'])
		self.engine.execute("INSERT INTO {} VALUES (?)", [test_table], ['cba'])
		res = self.engine.execute("SELECT col1 FROM {} WHERE col1 " +
			self.engine.getRegexpOp() + " ?", [test_table], ['a\w+'])
		self.assertEqual(res, [('abc',), ('bac',)])

	@unittest.skip("Not all DB engines support it")
	def testRegexpSupportInBlob(self):
		"""
		Check that engine supports regexp search in BLOB values

		Sqlite3 supports it, postgre does not (only using PL\something)
		"""
		test_table = 'ttt'
		bytes_type = self.engine.getColumnType(bytes())
		regexp = self.engine.getRegexpOp()
		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 " + bytes_type + ")", [test_table])
		for val in [b'\x00\x01\x02', b'\x01\x00\x02', b'\x01\x02\x00']:
			self.engine.execute("INSERT INTO {} VALUES (?)", [test_table], [val])
		res = self.engine.execute("SELECT col1 FROM {} WHERE col1 " + regexp + " ?",
			[test_table], [b'\x00.+'])
		self.assertEqual(res, [(b'\x00\x01\x02',), (b'\x01\x00\x02',)])

	def testUnicodeSupport(self):
		"""Check that DB can store and return unicode values"""
		austria = "\xd6sterreich"
		test_table = 'ttt'
		regexp = self.engine.getRegexpOp()

		self.engine.begin()
		self.engine.execute("CREATE TABLE {} (col1 " + self.str_type + ")", [test_table])
		self.engine.execute("INSERT INTO {} VALUES (?)", [test_table], [austria])

		# check that unicode string can be queried
		res = self.engine.execute("SELECT col1 FROM {}", [test_table])
		self.assertEqual(res, [(austria,)])

		# check that regexp works for unicode
		self.engine.execute("INSERT INTO {} VALUES (?)", [test_table], ['aaa'])
		res = self.engine.execute("SELECT col1 FROM {} WHERE col1 " + regexp + " ?",
			[test_table], [austria[:3]])
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
		self.engine.execute("CREATE TABLE {} (col1 " + self.str_type +
			", col2 " + self.str_type + ")", [test_table])
		self.engine.execute("INSERT INTO {} VALUES (?, ?)", [test_table], ['a', 'b'])
		self.engine.execute("INSERT INTO {} VALUES (?, ?)", [test_table], ['a', None])
		self.engine.execute("INSERT INTO {} VALUES (?, ?)", [test_table], [None, 'b'])
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

	def testGetTablesList(self):
		"""Check operation of getTablesList()"""
		test_tables = ['ttt1', 'ttt2', 'ttt3']
		self.engine.begin()
		for table in test_tables:
			self.engine.execute("CREATE TABLE {} (col1 " + self.str_type + ")", [table])

		tables_list = self.engine.getTablesList()

		# there can be DB service tables in list
		for table in test_tables:
			self.assertTrue(table in tables_list)

	def testIsFieldTableName(self):
		"""Check that Field.isFieldTableName works"""
		f = Field(self.engine, ['aaa', 1])
		self.assertTrue(Field.isFieldTableName(self.engine, f.name_str))
		self.assertFalse(Field.isFieldTableName(self.engine, "blablabla"))

	def testSelectExistingTables(self):
		"""Check that engine.selectExistingTables works"""
		table_set1 = {'ttt1', 'ttt2', 'ttt3'}
		table_set2 = {'ttt2', 'ttt3', 'ttt4'}

		self.engine.begin()
		for table in table_set1:
			self.engine.execute("CREATE TABLE {} (col1 " + self.str_type + ")", [table])

		result_set = set(self.engine.selectExistingTables(table_set2))
		reference_set = table_set1.intersection(table_set2)
		self.assertEqual(result_set, reference_set)


class EngineTestParams:

	def __init__(self, engine_tag, storage_tag, in_memory, engine_args, engine_kwds):
		self.test_tag = engine_tag + "." + storage_tag
		self.in_memory = in_memory
		self.engine_tag = engine_tag
		self.engine_args = engine_args
		self.engine_kwds = engine_kwds


def getEngineTestParams(db_path, all_engines, all_storages):

	IN_MEMORY = 'memory' # tag for in-memory DB tests

	storages = {
		'sqlite3': [(IN_MEMORY, (None,), {}), ('file', ('test.db',),
			{'open_existing': 0, 'db_path': db_path})],
		'postgre': [('tempdb', ('tempdb',), {'open_existing': 0,
			'port': 5432, 'user': 'postgres', 'password': ''})]
	}

	if not all_storages:
		# leave only default storages
		storages = {x: [storages[x][0]] for x in storages}

	if not all_engines:
		default_tag = brain.getDefaultEngineTag()
		storages = {default_tag: storages[default_tag]}

	res = []

	for engine_tag in storages:
		for storage_tag, args, kwds in storages[engine_tag]:
			res.append(EngineTestParams(engine_tag, storage_tag,
				(storage_tag == IN_MEMORY), args, kwds))

	return res

def getParameterizedEngineTest(engine_tag, engine_args, engine_kwds):

	class Derived(EngineTest):
		def setUp(self):
			self.engine = getEngineByTag(engine_tag)(*engine_args, **engine_kwds)
			self.str_type = self.engine.getColumnType(str())

		def tearDown(self):
			self.engine.close()

	return Derived

def suite(db_path, all_engines, all_storages):

	res = helpers.NamedTestSuite('engine')

	for params in getEngineTestParams(db_path, all_engines, all_storages):
		engine_suite = helpers.NamedTestSuite(params.test_tag)
		engine_suite.addTestCaseClass(getParameterizedEngineTest(params.engine_tag,
			params.engine_args, params.engine_kwds))
		res.addTest(engine_suite)

	return res
