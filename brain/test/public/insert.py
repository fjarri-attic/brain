"""Unit tests for database layer insert request"""

import unittest

import brain
import brain.op as op

import helpers
from public.requests import TestRequest, getParameterized

class Insert(TestRequest):
	"""Test operation of InsertRequest"""

	def testToTheMiddleSimpleList(self):
		"""Check insertion to the middle of simple list"""
		self.prepareStandSimpleList()
		self.conn.insertMany(self.id1, ['tracks', 1], ['Track 4', 'Track 5'])
		res = self.conn.readByMask(self.id1, ['tracks', None])
		self.assertEqual(res, {'tracks': [
			'Track 1', 'Track 4', 'Track 5', 'Track 2', 'Track 3'
		]})

	def testToTheBeginningSimpleList(self):
		"""Check insertion to the beginning of simple list"""
		self.prepareStandSimpleList()
		self.conn.insertMany(self.id1, ['tracks', 0], ['Track 4', 'Track 5'])
		res = self.conn.readByMask(self.id1, ['tracks', None])
		self.assertEqual(res, {'tracks': [
			'Track 4', 'Track 5', 'Track 1', 'Track 2', 'Track 3'
		]})

	def testToTheEndSimpleList(self):
		"""Check insertion to the end of simple list"""
		self.prepareStandSimpleList()
		self.conn.insertMany(self.id1, ['tracks', None], ['Track 4', 'Track 5'])
		res = self.conn.readByMask(self.id1, ['tracks', None])
		self.assertEqual(res, {'tracks': [
			'Track 1', 'Track 2', 'Track 3', 'Track 4', 'Track 5'
		]})

	def testToTheMiddleNestedList(self):
		"""Test insertion to the middle of nested list"""
		self.prepareStandNestedList()
		self.conn.insertMany(self.id2, ['tracks', 1], [
			{'name': 'Track 4 name'}, {'name': 'Track 5 name'}
		])

		res = self.conn.readByMask(self.id2, ['tracks', None, 'name'])
		self.assertEqual(res, {'tracks': [
			{'name': 'Track 1 name'},
			{'name': 'Track 4 name'},
			{'name': 'Track 5 name'},
			{'name': 'Track 2 name'},
			{'name': 'Track 3 name'}
		]})

		res = self.conn.readByMask(self.id2, ['tracks', None, 'authors', None])
		self.assertEqual(res, {'tracks': [
			{'authors': ['Carl II', 'Dan']},
			None,
			None,
			{'authors': ['Alex']},
			{'authors': ['Rob']}
		]})

	def testToTheBeginningNestedList(self):
		"""Test insertion to the beginning of nested list"""
		self.prepareStandNestedList()
		self.conn.insertMany(self.id2, ['tracks', 0], [
			{'name': 'Track 4 name'}, {'name': 'Track 5 name'}
		])

		res = self.conn.readByMask(self.id2, ['tracks', None, 'name'])
		self.assertEqual(res, {'tracks': [
			{'name': 'Track 4 name'},
			{'name': 'Track 5 name'},
			{'name': 'Track 1 name'},
			{'name': 'Track 2 name'},
			{'name': 'Track 3 name'}
		]})

		res = self.conn.readByMask(self.id2, ['tracks', None, 'authors', None])
		self.assertEqual(res, {'tracks': [
			None,
			None,
			{'authors': ['Carl II', 'Dan']},
			{'authors': ['Alex']},
			{'authors': ['Rob']}
		]})

	def testToTheEndNestedList(self):
		"""Test insertion to the end of nested list"""
		self.prepareStandNestedList()
		self.conn.insertMany(self.id2, ['tracks', None], [
			{'name': 'Track 4 name'}, {'name': 'Track 5 name'}
		])

		res = self.conn.readByMask(self.id2, ['tracks', None, 'name'])
		self.assertEqual(res, {'tracks': [
			{'name': 'Track 1 name'},
			{'name': 'Track 2 name'},
			{'name': 'Track 3 name'},
			{'name': 'Track 4 name'},
			{'name': 'Track 5 name'}
		]})

		res = self.conn.readByMask(self.id2, ['tracks', None, 'authors', None])
		self.assertEqual(res, {'tracks': [
			{'authors': ['Carl II', 'Dan']},
			{'authors': ['Alex']},
			{'authors': ['Rob']},
		]})

	def testTreeToTheBeginningNestedList(self):
		"""Test insertion of the data tree to the beginning of nested list"""
		self.prepareStandNestedList()
		self.conn.insert(self.id2, ['tracks', 0], {'authors': ['Earl', 'Fred']})
		res = self.conn.readByMask(self.id2, ['tracks', None, 'name'])
		self.assertEqual(res, {'tracks': [
			None,
			{'name': 'Track 1 name'},
			{'name': 'Track 2 name'},
			{'name': 'Track 3 name'}
		]})

		res = self.conn.readByMask(self.id2, ['tracks', None, 'authors', None])
		self.assertEqual(res, {'tracks': [
			{'authors': ['Earl', 'Fred']},
			{'authors': ['Carl II', 'Dan']},
			{'authors': ['Alex']},
			{'authors': ['Rob']}
		]})

	def testTreeToTheEndNestedList(self):
		"""Test insertion of the data tree to the end of nested list"""
		self.prepareStandNestedList()
		self.conn.insert(self.id2, ['tracks', None], {'authors': ['Earl', 'Fred']})
		res = self.conn.readByMask(self.id2, ['tracks', None, 'name'])
		self.assertEqual(res, {'tracks': [
			{'name': 'Track 1 name'},
			{'name': 'Track 2 name'},
			{'name': 'Track 3 name'}
		]})

		res = self.conn.readByMask(self.id2, ['tracks', None, 'authors', None])
		self.assertEqual(res, {'tracks': [
			{'authors': ['Carl II', 'Dan']},
			{'authors': ['Alex']},
			{'authors': ['Rob']},
			{'authors': ['Earl', 'Fred']}
		]})

	def testToTheEndSeveralLists(self):
		"""Test insertion to the end of list when there are other lists on the same level"""
		self.prepareStandNestedList()

		res = self.conn.insertMany(self.id2,
			['tracks', 1, 'authors', None],
			['Yngwie', 'Zack'])

		res = self.conn.readByMask(self.id2, ['tracks', None, 'authors', None])
		self.assertEqual(res, {'tracks': [
			{'authors': ['Carl II', 'Dan']},
			{'authors': ['Alex', 'Yngwie', 'Zack']},
			{'authors': ['Rob']}
		]})

	def testToEmptyList(self):
		"""Check that insertion to non-existing list creates this list"""
		self.prepareStandSimpleList()

		self.conn.insertMany(self.id2,
			['tracks', 2, 'authors', None],
			['Earl', 'Fred'], remove_conflicts=True)

		res = self.conn.readByMask(self.id2, ['tracks', None, 'authors', None])
		self.assertEqual(res, {'tracks': [
			None,
			None,
			{'authors': ['Earl', 'Fred']}
		]})

	def testToListWithSeveralTypes(self):
		"""
		Check that several values of different types can be inserted
		to list which already has values of several types
		"""
		self.prepareStandDifferentTypes()
		self.conn.insertMany(self.id1, ['meta', 2], ['Monk', 2, 10.0])
		res = self.conn.readByMask(self.id1, ['meta', None])
		self.assertEqual(res, {'meta': [
			'Pikeman', 'Archer', 'Monk', 2,
			10.0, 1, 2, 4.0, 5.0, b'Gryphon', b'Swordsman'
		]})

	def testWrongRenumber(self):
		"""
		Regression test for bug when special characters in field name (namely,
		separating dots) were not escaped when searching for child fields using
		regexp. As a result, getFieldsList() returned two copies of one field,
		and renumber moved list element to wrong position during insertion.
		"""
		data = {'key1': [ [[20]], {'key2': [30]} ]}
		to_add = 'aaa'
		obj = self.conn.create(data)
		self.conn.insert(obj, ['key1', 0, 0], to_add)
		res = self.conn.read(obj)
		data['key1'][0].insert(0, to_add)
		self.assertEqual(res, data)

	def testInsertToMap(self):
		"""
		Regression test for bug, when one could insert data to map,
		creating mixed list-map structure
		"""
		obj = self.conn.create({'key': [1, 2, 3]})
		self.assertRaises(brain.StructureError, self.conn.insert,
			obj, [0], 'val')

	def testWrongAutovivification(self):
		"""
		Regression test for bug when autovivified path during insert
		spoiled database structure
		"""
		obj = self.conn.create({'key': [1, 2, 3]})
		self.assertRaises(brain.StructureError, self.conn.insert,
			obj, ['key', 'key2', 0], 'val')

	def testRootAutovivification(self):
		"""Test that autovivification properly handles root data structures"""
		obj = self.conn.create({'key': [1, 2, 3]})
		self.conn.insert(obj, ['key2', None], 'val')
		res = self.conn.read(obj)
		self.assertEqual(res, {'key': [1, 2, 3], 'key2': ['val']})

	def testInsertWithRemovingConflicts(self):
		"""Simple test to check that remove_conflicts option works"""
		obj = self.conn.create({'key': [1, 2, 3]})
		self.conn.insert(obj, ['key', 'key2', None], 'val', True)
		res = self.conn.read(obj)
		self.assertEqual(res, {'key': {'key2': ['val']}})

	def testRemoveConflictsPreservesFields(self):
		"""Check that remove_conflicts option keeps non-conflicting fields"""
		obj = self.conn.create({'key': [1, 2, 3]})
		self.conn.insert(obj, ['key2', 'key3', None], 'val', True)
		res = self.conn.read(obj)
		self.assertEqual(res, {'key2': {'key3': ['val']}, 'key': [1, 2, 3]})

	def testSpoiledStructureAfterConflictRemoval(self):
		"""
		Regression test for bug when conflict removal during insert
		did not remove all conflicting structures
		"""
		obj = self.conn.create({'key2': [50]})
		self.conn.insert(obj, ['key2', 'key3', None], 50, remove_conflicts=True)
		self.assertRaises(brain.StructureError, self.conn.insert,
			obj, ['key2', None], 51)

	def testAutoexpandNewList(self):
		"""
		Check that when new list is created before insertion, it is autoexpanded
		to the index, where path points to.
		"""
		obj = self.conn.create({'key': [1, 2, 3]})
		self.conn.insertMany(obj, [1], [4, 5, 6], remove_conflicts=True)
		res = self.conn.read(obj)
		self.assertEqual(res, [None, 4, 5, 6])

	def testAutoexpandNewNestedLists(self):
		"""
		Check that when several nested lists are created before insertion,
		they are autoexpanded to the index, where path points to.
		"""
		obj = self.conn.create({'key': [1, 2, 3]})
		self.conn.insertMany(obj, [1, 2], [4, 5, 6], remove_conflicts=True)

		self.assertEqual(self.conn.read(obj), [None, [None, None, 4, 5, 6]])
		self.assertEqual(self.conn.read(obj, [0]), None)
		self.assertEqual(self.conn.read(obj, [1, 0]), None)


def suite(engine_params, connection_generator):
	res = helpers.NamedTestSuite('insert')
	res.addTestCaseClass(getParameterized(Insert, engine_params, connection_generator))
	return res
