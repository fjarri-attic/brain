"""Unit tests for database facade"""

import unittest
import copy

import brain
import brain.op as op

import helpers
from public.requests import TestRequest, getParameterized

class Connection(TestRequest):
	"""Test different uses of ModifyRequest"""

	def testAutocommitNoErrors(self):
		"""Check autocommit works when there are no exceptions raised"""

		data = {'name': 'Alex', 'age': 22}
		obj = self.conn.create(data)
		res = self.conn.read(obj)
		self.assertEqual(res, data)

	def testAutocommitRollsBackOnError(self):
		"""Check that in autocommit mode there will be rollback if action raises exception"""
		data = {'name': 'Alex', 'friends': ['Bob', 'Carl']}
		obj = self.conn.create(data)

		# this should raise a error, because we are trying
		# to create map where there is already a list
		self.assertRaises(brain.StructureError, self.conn.modify,
			obj, ['friends', 'fld'], 2)

		res = self.conn.read(obj)
		self.assertEqual(res, data)

	def testAsyncTransactionNoErros(self):
		"""Check asynhcronous transaction operation when there aren't any errors"""

		data1 = {'name': 'Alex', 'friends': ['Bob', 'Carl'],
			'bytedata': b'bbb'}
		data2 = {'name': 'Roy', 'friends': ['Ned', 'Mark']}

		# create two objects
		self.conn.beginAsync()
		self.conn.create(data1)
		self.conn.create(data2)
		results = self.conn.commit()

		# results should contain IDs of created objects
		obj1_data = self.conn.read(results[0])
		obj2_data = self.conn.read(results[1])

		self.assertEqual(obj1_data, data1)
		self.assertEqual(obj2_data, data2)

	def testAsyncTransactionRollback(self):
		"""Check asynchronous transaction rollback"""
		self.prepareStandNoList()

		self.conn.beginAsync()
		self.conn.modify(self.id1, ['name'], 'Zed')
		self.conn.rollback()

		# self.id1 should have remained unchanged
		res = self.conn.read(self.id1)
		self.assertEqual(res, {'name': 'Alex', 'phone': '1111'})

	def testAsyncTransactionError(self):
		"""Check that asynchronous transaction is rolled back completely on error"""
		self.prepareStandSimpleList()

		self.conn.beginAsync()
		self.conn.modify(self.id2, ['tracks', 'fld'], 'RRR') # this will raise an exception
		self.conn.modify(self.id1, ['name'], 'Zed')
		self.assertRaises(brain.StructureError, self.conn.commit)

		# self.id1 should have remained unchanged
		res = self.conn.read(self.id2)
		self.assertEqual(res, {'tracks': ['Track 2', 'Track 1']})

	def testAsyncTransactionReturnValues(self):
		"""Check that requests return expected values after asyncronous transaction"""

		self.prepareStandSimpleList()
		data = {'name': 'Earl', 'friends': ['Cat', 'Dog']}

		# test all possible requests
		self.conn.beginAsync()
		self.conn.modify(self.id1, ['name'], 'Zed')
		self.conn.create(data)
		self.conn.read(self.id2)
		self.conn.search(['name'], op.EQ, 'Zed')
		self.conn.insert(self.id1, ['tracks', None], 'Track 4')
		self.conn.insertMany(self.id2, ['tracks', None], ['Track 3', 'Track 4'])
		results = self.conn.commit()

		# check that results list has expected contents

		# modify() should have returned None
		self.assertEqual(results[0], None)

		# create() should have returned new object ID
		res = self.conn.read(results[1])
		self.assertEqual(res, data)

		# read() should have returned object contents
		self.assertEqual(results[2], {'tracks': ['Track 2', 'Track 1']})

		# search() should have returned object 1 ID (we added name Zed to it earlier)
		self.assertEqual(results[3], [self.id1])

		# insert() and insertMany() should have returned None
		self.assertEqual(results[4], None)
		self.assertEqual(results[5], None)

	def testSyncTransactionNoErrors(self):
		"""Check synchronous transaction operation when there are no errors"""

		data1 = {'name': 'Alex', 'friends': ['Bob', 'Carl']}
		data2 = {'name': 'Roy', 'friends': ['Ned', 'Mark']}

		self.conn.beginSync()
		obj1 = self.conn.create(data1)
		obj2 = self.conn.create(data2)
		obj1_data = self.conn.read(obj1)
		obj2_data = self.conn.read(obj2)
		self.conn.commit()

		self.assertEqual(obj1_data, data1)
		self.assertEqual(obj2_data, data2)

	def testSyncTransactionRollback(self):
		"""Check that synchronous transaction operation can be rolled back"""
		self.prepareStandNoList()

		self.conn.beginSync()
		self.conn.modify(self.id1, None, {'name': 'Zack'})
		self.conn.rollback()

		# check that rollback really occurred
		res = self.conn.read(self.id1)
		self.assertEqual(res, {'name': 'Alex', 'phone': '1111'})

	def testSyncTransactionError(self):
		"""Check that sync transaction automatically rolls back on error"""
		self.prepareStandSimpleList()

		self.conn.beginSync()
		self.conn.modify(self.id2, None, {'name': 'Alex'})

		# this will raise an exception
		self.assertRaises(brain.StructureError, self.conn.modify,
			self.id1, ['tracks', 'fld'], 0)

		# check that transaction already ended
		self.assertRaises(brain.FacadeError, self.conn.rollback)

		# check that rollback really occurred
		res = self.conn.read(self.id2)
		self.assertEqual(res, {'tracks': ['Track 2', 'Track 1']})

	def testBeginDuringTransaction(self):
		"""Check that begin() raises proper exception when transaction is in progress"""
		self.conn.beginAsync()
		self.assertRaises(brain.FacadeError, self.conn.beginAsync)
		self.assertRaises(brain.FacadeError, self.conn.beginSync)
		self.conn.commit()

		self.conn.beginSync()
		self.assertRaises(brain.FacadeError, self.conn.beginAsync)
		self.assertRaises(brain.FacadeError, self.conn.beginSync)
		self.conn.commit()

	def testCommitOrRollbackWhenNoTransaction(self):
		"""
		Check that commit() and rollback() raise proper exception
		when transaction is not in progress
		"""
		self.assertRaises(brain.FacadeError, self.conn.commit)
		self.assertRaises(brain.FacadeError, self.conn.rollback)

	def testObjectExistsRequest(self):
		"""
		Check that objectExists() returns True for existing objects
		and None for non-existent
		"""
		self.prepareStandNoList()
		self.conn.delete(self.id1)
		self.assertEqual(self.conn.objectExists(self.id1), False)
		self.assertEqual(self.conn.objectExists(self.id2), True)

	def testKeywordArgumentsInRequest(self):
		"""
		Check that requests with keyword arguments can pass through
		remote connections like XML RPC
		"""
		obj = self.conn.create(1, path=['fld'])
		res = self.conn.read(obj)
		self.assertEqual(res, {'fld': 1})

	def testKeywordArgumentsInMethod(self):
		"""
		Check that method calls with keyword arguments can pass through
		remote connections, like XML RPC
		"""
		conn2 = self.gen.connect(self.gen.getDefaultEngineTag(), name=None)
		conn2.close()

	def testDump(self):
		"""Check dumping to Python structures"""
		self.prepareStandNoList()
		res = self.conn.dump()

		# separate IDs from data and create a dictionary
		# for easier comparison
		dump_dict = {obj_id: data for obj_id, data in zip(res[::2], res[1::2])}

		reference_data = {
			self.id1: {'name': 'Alex', 'phone': '1111'},
			self.id2: {'name': 'Bob', 'phone': '2222'},
			self.id3: {'name': 'Carl', 'phone': '3333', 'age': '27'},
			self.id4: {'name': 'Don', 'phone': '4444', 'age': '20'},
			self.id5: {'name': 'Alex', 'phone': '1111', 'age': '22'}
		}

		self.assertEqual(dump_dict, reference_data)

	def testReferences(self):
		"""Check that object IDs can be saved in database"""
		obj = self.conn.create({'test': 'val'})
		obj2 = self.conn.create({'ref': obj})

		# try to read reference
		data2 = self.conn.read(obj2)
		self.assertEqual(data2, {'ref': obj})

		# try to use reference
		data = self.conn.read(data2['ref'])
		self.assertEqual(data, {'test': 'val'})

	def testSecondConnection(self):
		"""Check that second connection to DB can see changes after commit in first connection"""

		# this test makes no sense for in-memory databases - they allow only one connection
		if self.in_memory: return

		data = {'name': 'Alex', 'age': 22}
		obj = self.conn.create(data)

		# create second connection
		args = self.connection_args
		kwds = copy.deepcopy(self.connection_kwds)
		kwds['open_existing'] = 1
		conn2 = self.gen.connect(self.tag, *args, **kwds)
		res = conn2.read(obj)
		conn2.close()

		self.assertEqual(res, data)

	def testWrongEngineTag(self):
		"""Check that error is thrown if wrong engine tag is provided"""
		self.assertRaises(brain.FacadeError, brain.connect, 'wrong_tag')

	def testRepair(self):
		"""Check that repair request at least does not spoil anything"""
		self.prepareStandDifferentTypes()
		data_before = self.conn.dump()

		# separate IDs from data and create a dictionary
		# for easier comparison
		data_before_dict = {obj_id: data for obj_id, data in
			zip(data_before[::2], data_before[1::2])}

		self.conn.repair()

		data_after = self.conn.dump()
		data_after_dict = {obj_id: data for obj_id, data in
			zip(data_after[::2], data_after[1::2])}

		self.assertEqual(data_before, data_after)


def suite(engine_params, connection_generator):
	res = helpers.NamedTestSuite('connection')
	res.addTestCaseClass(getParameterized(Connection, engine_params, connection_generator))
	return res
