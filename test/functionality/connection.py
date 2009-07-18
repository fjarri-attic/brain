"""Unit tests for database facade"""

import unittest

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import brain
import brain.op as op
from functionality.requests import TestRequest

class Connection(TestRequest):
	"""Test different uses of ModifyRequest"""

	def testAutocommitNoErrors(self):
		"""Check autocommit works when there are no exceptions raised"""

		# we cannot test autocommit on in-memory database
		if self.db == None: return

		data = {'name': 'Alex', 'age': 22}
		obj = self.conn.create(data)

		# now open another connection to this database and check that
		# changes were really made
		conn2 = brain.connect(self.db)
		res = conn2.read(obj)
		conn2.close()
		self.assertEqual(res, data)

	def testAutocommitRollsBackOnError(self):
		"""Check that in autocommit mode there will be rollback if action raises exception"""
		data = {'name': 'Alex', 'friends': ['Bob', 'Carl']}
		obj = self.conn.create(data)

		# this should raise a error, because we are trying
		# to create map where there is already a list
		self.assertRaises(brain.StructureError, self.conn.modify,
			obj, 2, ['friends', 'fld'])

		res = self.conn.read(obj)
		self.assertEqual(res, data)

	def testAsyncTransactionNoErros(self):
		"""Check asynhcronous transaction operation when there aren't any errors"""

		data1 = {'name': 'Alex', 'friends': ['Bob', 'Carl']}
		data2 = {'name': 'Roy', 'friends': ['Ned', 'Mark']}

		# create two objects
		self.conn.begin()
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

		self.conn.begin()
		self.conn.modify(self.id1, 'Zed', ['name'])
		self.conn.rollback()

		# self.id1 should have remained unchanged
		res = self.conn.read(self.id1)
		self.assertEqual(res, {'name': 'Alex', 'phone': '1111'})

	def testAsyncTransactionError(self):
		"""Check that asynchronous transaction is rolled back completely on error"""
		self.prepareStandSimpleList()

		self.conn.begin()
		self.conn.modify(self.id2, 'RRR', ['tracks', 'fld']) # this will raise an exception
		self.conn.modify(self.id1, 'Zed', ['name'])
		self.assertRaises(brain.StructureError, self.conn.commit)

		# self.id1 should have remained unchanged
		res = self.conn.read(self.id2)
		self.assertEqual(res, {'tracks': ['Track 2', 'Track 1']})

	def testAsyncTransactionReturnValues(self):
		"""Check that requests return expected values after asyncronous transaction"""

		self.prepareStandSimpleList()
		data = {'name': 'Earl', 'friends': ['Cat', 'Dog']}

		# test all possible requests
		self.conn.begin()
		self.conn.modify(self.id1, 'Zed', ['name'])
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
		self.conn.modify(self.id1, {'name': 'Zack'})
		self.conn.rollback()

		# check that rollback really occurred
		res = self.conn.read(self.id1)
		self.assertEqual(res, {'name': 'Alex', 'phone': '1111'})

	def testSyncTransactionError(self):
		"""Check that sync transaction automatically rolls back on error"""
		self.prepareStandSimpleList()

		self.conn.beginSync()
		self.conn.modify(self.id2, {'name': 'Alex'})

		# this will raise an exception
		self.assertRaises(brain.StructureError, self.conn.modify,
			self.id1, 0, ['tracks', 'fld'])

		# check that transaction already ended
		self.assertRaises(brain.FacadeError, self.conn.rollback)

		# check that rollback really occurred
		res = self.conn.read(self.id2)
		self.assertEqual(res, {'tracks': ['Track 2', 'Track 1']})

	def testBeginDuringTransaction(self):
		"""Check that begin() raises proper exception when transaction is in progress"""
		self.conn.begin()
		self.assertRaises(brain.FacadeError, self.conn.begin)
		self.assertRaises(brain.FacadeError, self.conn.beginSync)
		self.conn.commit()

		self.conn.beginSync()
		self.assertRaises(brain.FacadeError, self.conn.begin)
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
		conn2 = self.gen.connect(None, engine_tag=self.gen.getDefaultEngineTag())
		conn2.close()

	def testDump(self):
		"""Check dumping to Python structures"""
		self.prepareStandNoList()
		res = self.conn.dump()
		self.assertEqual(res, {
	 		self.id1: {'name': 'Alex', 'phone': '1111'},
			self.id2: {'name': 'Bob', 'phone': '2222'},
			self.id3: {'name': 'Carl', 'phone': '3333', 'age': '27'},
			self.id4: {'name': 'Don', 'phone': '4444', 'age': '20'},
			self.id5: {'name': 'Alex', 'phone': '1111', 'age': '22'}
		})


def get_class():
	return Connection
