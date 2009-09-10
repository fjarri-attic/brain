"""Unit tests for Delete requests"""

import unittest

import brain
import brain.op as op

import helpers
from public.requests import TestRequest, getParameterized

class Delete(TestRequest):
	"""Test operation of DeleteRequest"""

	def testWholeObject(self):
		"""Check deletion of the whole object"""
		self.prepareStandNoList()

		self.conn.delete(self.id3)

		# check that field of deleted object is gone
		res = self.conn.search(['phone'], op.EQ, '3333')
		self.assertEqual(res, [])

		# Check that other objects are intact
		res = self.conn.search(['phone'], op.EQ, '1111')
		self.assertSameElements(res, [self.id1, self.id5])

	def testExistentFields(self):
		"""Check that deletion of existent fields preserves other object fields"""
		self.prepareStandNoList()

		self.conn.deleteMany(self.id3, [['age'], ['phone']])

		# Check that other fields are intact
		res = self.conn.search(['name'], op.EQ, 'Carl')
		self.assertEqual(res, [self.id3])

		# Check that fields were really deleted
		res = self.conn.search(['phone'], op.EQ, '3333')
		self.assertEqual(res, [])

		res = self.conn.search(['age'], op.EQ, '27')
		self.assertEqual(res, [])

	def testNonExistentFields(self):
		"""Check deletion of non-existent fields"""
		self.prepareStandNoList()

		self.conn.deleteMany(self.id2, [['name'], ['blablabla']])

		# Check that existent field was deleted
		res = self.conn.search(['name'], op.EQ, 'Bob')
		self.assertEqual(res, [])

		# Check that other fields are intact
		res = self.conn.search(['phone'], op.EQ, '2222')
		self.assertEqual(res, [self.id2])

	def testAllObjects(self):
		"""Test that deleting all objects does not spoil the database"""
		self.prepareStandNoList()

		# Remove all
		self.conn.delete(self.id1)
		self.conn.delete(self.id2)
		self.conn.delete(self.id3)
		self.conn.delete(self.id4)
		self.conn.delete(self.id5)

		# Add object again
		obj = self.conn.create({'name': 'Alex', 'phone': '2222'})

		# Check that addition was successful
		res = self.conn.search(['phone'], op.EQ, '2222')
		self.assertEqual(res, [obj])

	def testSimpleListFromMiddle(self):
		"""Test deletion from the middle of the list"""
		self.prepareStandSimpleList()
		self.conn.delete(self.id1, ['tracks', 1])
		res = self.conn.readByMask(self.id1, ['tracks', None])
		self.assertEqual(res, {'tracks': ['Track 1', 'Track 3']})

	def testSimpleListFromBeginning(self):
		"""Test deletion from the beginning of the list"""
		self.prepareStandSimpleList()
		self.conn.delete(self.id1, ['tracks', 0])
		res = self.conn.readByMask(self.id1, ['tracks', None])
		self.assertEqual(res, {'tracks': ['Track 2', 'Track 3']})

	def testSimpleListFromEnd(self):
		"""Test deletion from the end of the list"""
		self.prepareStandSimpleList()
		self.conn.delete(self.id1, ['tracks', 2])
		res = self.conn.readByMask(self.id1, ['tracks', None])
		self.assertEqual(res, {'tracks': ['Track 1', 'Track 2']})

	def testNestedListFromMiddle(self):
		"""Test deletion from the middle of the nested list"""
		self.prepareStandNestedList()
		self.conn.delete(self.id2, ['tracks', 1])

		# Check that deletion and reenumeration occurred
		res = self.conn.readByMask(self.id2, ['tracks', None, 'Name'])
		self.assertEqual(res, {'tracks': [
			{'Name': 'Track 1 name'},
			{'Name': 'Track 3 name'}
		]})

		# Check that nested list is intact and reenumeration occurred in it too
		res = self.conn.readByMask(self.id2, ['tracks', None, 'Authors', None])
		self.assertEqual(res, {'tracks': [
			{'Authors': ['Carl II', 'Dan']},
			{'Authors': ['Rob']}
		]})

	def testNestedListFromBeginning(self):
		"""Test deletion from the beginning of the nested list"""
		self.prepareStandNestedList()
		self.conn.delete(self.id2, ['tracks', 0])

		# Check that deletion and reenumeration occurred
		res = self.conn.readByMask(self.id2, ['tracks', None, 'Name'])
		self.assertEqual(res, {'tracks': [
			{'Name': 'Track 2 name'},
			{'Name': 'Track 3 name'}
		]})

		# Check that nested list is intact and reenumeration occurred in it too
		res = self.conn.readByMask(self.id2, ['tracks', None, 'Authors', None])
		self.assertEqual(res, {'tracks': [
			{'Authors': ['Alex']}, {'Authors': ['Rob']}
		]})

	def testNestedListFromEnd(self):
		"""Test deletion from the end of the nested list"""
		self.prepareStandNestedList()
		self.conn.delete(self.id2, ['tracks', 2])

		# Check that deletion and reenumeration occurred
		res = self.conn.readByMask(self.id2, ['tracks', None, 'Name'])
		self.assertEqual(res, {'tracks': [
			{'Name': 'Track 1 name'},
			{'Name': 'Track 2 name'}
		]})

		# Check that nested list is intact and reenumeration occurred in it too
		res = self.conn.readByMask(self.id2, ['tracks', None, 'Authors', None])
		self.assertEqual(res, {'tracks': [
			{'Authors': ['Carl II', 'Dan']},
			{'Authors': ['Alex']}
		]})

	def testFromListByMaskLeaf(self):
		"""Test deletion using list mask, leaf list"""
		self.prepareStandNestedList()
		self.conn.delete(self.id2, ['tracks', 0, 'Authors', None])
		res = self.conn.readByMask(self.id2, ['tracks', None, 'Authors', None])
		self.assertEqual(res, {'tracks': [
			None, {'Authors': ['Alex']}, {'Authors': ['Rob']}
		]})

	def testFromListByMask(self):
		"""Test deletion using list mask, non-leaf list"""
		self.prepareStandNestedList()
		self.conn.delete(self.id2, ['tracks', None, 'Authors', 0])
		res = self.conn.readByMask(self.id2, ['tracks', None, 'Authors', None])
		self.assertEqual(res, {'tracks': [{'Authors': ['Dan']}]})

	def testFromListKeepsNeighbors(self):
		"""
		Regression test for bug when deleting element from list deletes all neighbors
		from its level of hierarchy
		"""
		self.prepareStandNestedList()
		self.conn.delete(self.id2, ['tracks', 0, 'Authors', 0])
		res = self.conn.readByMask(self.id2, ['tracks', None, 'Authors', 0])
		self.assertEqual(res, {'tracks': [
			{'Authors': ['Dan']},
			{'Authors': ['Alex']},
			{'Authors': ['Rob']}
		]})

	def testAllValuesFromField(self):
		"""Check that after we delete all values from some field of all objects, database is not spoiled"""
		self.prepareStandNoList()
		self.conn.delete(self.id3, ['age'])
		self.conn.delete(self.id4, ['age'])
		self.conn.delete(self.id5, ['age'])

		# check that searching in this table does not produce errors
		res = self.conn.search(['age'], op.EQ, '22')
		self.assertEqual(res, [])

		# check that we can create this table back
		self.conn.modify(self.id3, ['age'], '22')

		res = self.conn.search(['age'], op.EQ, '22')
		self.assertEqual(res, [self.id3])

	def testEmptyTableAfterRenumbering(self):
		"""
		Check that the situation when all values are deleted from some field after renumbering
		does not break the database
		"""
		self.prepareStandNestedList()
		self.conn.delete(self.id1, ['tracks', 2])

		# check that value was deleted
		res = self.conn.search(['tracks', None, 'Lyrics', None], op.EQ, 'Lalala')
		self.assertEqual(res, [])

		# add this value back
		self.conn.modify(self.id1, ['tracks', 2, 'Lyrics', 0], 'Blablabla')
		res = self.conn.search(['tracks', None, 'Lyrics', None], op.EQ, 'Blablabla')
		self.assertEqual(res, [self.id1])

	def testNonExistingObject(self):
		"""Check that deletion of non-existing object does not raise anything"""
		self.prepareStandNoList()
		self.conn.delete(6)

	def testSeveralTypesAtOnce(self):
		"""Check that values of different types can be deleted at once by mask"""
		self.prepareStandDifferentTypes()
		self.conn.delete(self.id1, ['meta', None])
		self.assertRaises(brain.LogicError, self.conn.readByMask, self.id1, ['meta', None])

	def testNoneValue(self):
		"""Check that Null values can be deleted"""
		obj = self.conn.create({'fld1': [None, 1]})
		self.conn.delete(obj, ['fld1', 0])
		res = self.conn.read(obj)
		self.assertEqual(res, {'fld1': [1]})

	def testListInTheMiddle(self):
		"""Regression test for incorrect Field.pointsToListElement() work"""
		self.prepareStandNestedList()

		# here pointsToListElements() returned true, because the last list index
		# is defined; but it is not the last name element, so the field really
		# does not point to list
		self.conn.delete(self.id1, ['tracks', 0, 'Name'])
		res = self.conn.readByMask(self.id1, ['tracks', None, 'Name'])
		self.assertEqual(res, {'tracks': [None, {'Name': 'Track 2 name'}]})

	def testSubTree(self):
		"""Check that one can delete a subtree at once"""
		self.prepareStandDifferentTypes()
		self.conn.delete(self.id1, ['tracks', 0])
		res = self.conn.readByMask(self.id1, ['tracks', None])
		self.assertEqual(res, {'tracks': [{
			'Name': 'Track 2 name',
			'Length': 350.0,
			'Volume': 26,
			'Rating': 4,
			'Authors': ['Carl', 'Dan'],
			'Data': b'\x00\x01\x03'
		}]})

	def testEmptyMap(self):
		"""Check that empty map remains in DB after deletion of elements"""
		data = {'aaa': 'bbb', 'parent': {'key': {'subkey': 'ccc'}}}
		obj = self.conn.create(data)
		self.conn.delete(obj, ['parent', 'key'])
		del data['parent']['key']
		res = self.conn.read(obj)
		self.assertEqual(res, data)

	def testEmptyList(self):
		"""Check that empty list remains in DB after deletion of elements"""
		data = {'aaa': 'bbb', 'parent': [[{'subkey': 'ccc'}]]}
		obj = self.conn.create(data)
		self.conn.delete(obj, ['parent', 0])
		del data['parent'][0]
		res = self.conn.read(obj)
		self.assertEqual(res, data)

	def testDeleteListsizes(self):
		"""
		Check that when list is deleted, corresponding
		listsizes table is removed too
		"""
		obj = self.conn.create({'aaa': [1, 2, 3], 'bbb': 'ccc'})
		self.conn.delete(obj, ['aaa'])
		self.conn.modify(obj, ['aaa'], [1])

		# insert value to the end of the list; if information
		# about the original list was not removed from the database,
		# it will think that the length of the list is 3, and store
		# new value in the wrong position
		self.conn.insert(obj, ['aaa', None], 2)
		res = self.conn.read(obj)
		self.assertEqual(res, {'aaa': [1, 2], 'bbb': 'ccc'})

	def testRenumberListsizes(self):
		"""
		Regression test, which checks that if list is moved due to deletion of element
		in parent list, it is handled correctly
		"""
		obj = self.conn.create({'key': 777})
		self.conn.modify(obj, ['key'], [1, 2, [None]])

		# this request should renumber listsizes table too
		self.conn.delete(obj, ['key', 0])

		# if listsizes remained unchanges, the following request will
		# insert value to wrong position - this applies both
		# to parent and child list
		self.conn.insert(obj, ['key', None], 3)
		self.conn.insert(obj, ['key', 1, None], 4)

		res = self.conn.read(obj)
		self.assertEqual(res, {'key': [2, [None, 4], 3]})

	def testDecreaseRefcounterForNull(self):
		"""
		Regression test for bug when deleting a None did not decrease
		corresponding refcount (because databases ignore =NULL condition)
		"""
		obj = self.conn.create([None, None, [1]])
		self.conn.delete(obj, [1])
		self.conn.delete(obj, [0])
		res = self.conn.read(obj)
		self.assertEqual(res, [[1]])

	def testNestedEmptyStructures(self):
		"""Check nested empty structures do not disappear after elements deletion"""
		obj = self.conn.create(['aaa', [[2], 1], 'bbb'])
		self.conn.delete(obj, [1, 0, 0])
		self.conn.delete(obj, [1, 1])
		res = self.conn.read(obj)
		self.assertEqual(res, ['aaa', [[]], 'bbb'])


def suite(engine_params, connection_generator):
	res = helpers.NamedTestSuite('delete')
	res.addTestCaseClass(getParameterized(Delete, engine_params, connection_generator))
	return res
