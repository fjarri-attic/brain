"""Unit tests for database layer insert request"""

import unittest

import brain
import brain.op as op
from brain.test.public.requests import TestRequest

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
			{'Name': 'Track 4 name'}, {'Name': 'Track 5 name'}
		])

		res = self.conn.readByMask(self.id2, ['tracks', None, 'Name'])
		self.assertEqual(res, {'tracks': [
			{'Name': 'Track 1 name'},
			{'Name': 'Track 4 name'},
			{'Name': 'Track 5 name'},
			{'Name': 'Track 2 name'},
			{'Name': 'Track 3 name'}
		]})

		res = self.conn.readByMask(self.id2, ['tracks', None, 'Authors', None])
		self.assertEqual(res, {'tracks': [
			{'Authors': ['Carl II', 'Dan']},
			None,
			None,
			{'Authors': ['Alex']},
			{'Authors': ['Rob']}
		]})

	def testToTheBeginningNestedList(self):
		"""Test insertion to the beginning of nested list"""
		self.prepareStandNestedList()
		self.conn.insertMany(self.id2, ['tracks', 0], [
			{'Name': 'Track 4 name'}, {'Name': 'Track 5 name'}
		])

		res = self.conn.readByMask(self.id2, ['tracks', None, 'Name'])
		self.assertEqual(res, {'tracks': [
			{'Name': 'Track 4 name'},
			{'Name': 'Track 5 name'},
			{'Name': 'Track 1 name'},
			{'Name': 'Track 2 name'},
			{'Name': 'Track 3 name'}
		]})

		res = self.conn.readByMask(self.id2, ['tracks', None, 'Authors', None])
		self.assertEqual(res, {'tracks': [
			None,
			None,
			{'Authors': ['Carl II', 'Dan']},
			{'Authors': ['Alex']},
			{'Authors': ['Rob']}
		]})

	def testToTheEndNestedList(self):
		"""Test insertion to the end of nested list"""
		self.prepareStandNestedList()
		self.conn.insertMany(self.id2, ['tracks', None], [
			{'Name': 'Track 4 name'}, {'Name': 'Track 5 name'}
		])

		res = self.conn.readByMask(self.id2, ['tracks', None, 'Name'])
		self.assertEqual(res, {'tracks': [
			{'Name': 'Track 1 name'},
			{'Name': 'Track 2 name'},
			{'Name': 'Track 3 name'},
			{'Name': 'Track 4 name'},
			{'Name': 'Track 5 name'}
		]})

		res = self.conn.readByMask(self.id2, ['tracks', None, 'Authors', None])
		self.assertEqual(res, {'tracks': [
			{'Authors': ['Carl II', 'Dan']},
			{'Authors': ['Alex']},
			{'Authors': ['Rob']},
		]})

	def testTreeToTheBeginningNestedList(self):
		"""Test insertion of the data tree to the beginning of nested list"""
		self.prepareStandNestedList()
		self.conn.insert(self.id2, ['tracks', 0], {'Authors': ['Earl', 'Fred']})
		res = self.conn.readByMask(self.id2, ['tracks', None, 'Name'])
		self.assertEqual(res, {'tracks': [
			None,
			{'Name': 'Track 1 name'},
			{'Name': 'Track 2 name'},
			{'Name': 'Track 3 name'}
		]})

		res = self.conn.readByMask(self.id2, ['tracks', None, 'Authors', None])
		self.assertEqual(res, {'tracks': [
			{'Authors': ['Earl', 'Fred']},
			{'Authors': ['Carl II', 'Dan']},
			{'Authors': ['Alex']},
			{'Authors': ['Rob']}
		]})

	def testTreeToTheEndNestedList(self):
		"""Test insertion of the data tree to the end of nested list"""
		self.prepareStandNestedList()
		self.conn.insert(self.id2, ['tracks', None], {'Authors': ['Earl', 'Fred']})
		res = self.conn.readByMask(self.id2, ['tracks', None, 'Name'])
		self.assertEqual(res, {'tracks': [
			{'Name': 'Track 1 name'},
			{'Name': 'Track 2 name'},
			{'Name': 'Track 3 name'}
		]})

		res = self.conn.readByMask(self.id2, ['tracks', None, 'Authors', None])
		self.assertEqual(res, {'tracks': [
			{'Authors': ['Carl II', 'Dan']},
			{'Authors': ['Alex']},
			{'Authors': ['Rob']},
			{'Authors': ['Earl', 'Fred']}
		]})

	def testToTheEndSeveralLists(self):
		"""Test insertion to the end of list when there are other lists on the same level"""
		self.prepareStandNestedList()

		res = self.conn.insertMany(self.id2,
			['tracks', 1, 'Authors', None],
			['Yngwie', 'Zack'])

		res = self.conn.readByMask(self.id2, ['tracks', None, 'Authors', None])
		self.assertEqual(res, {'tracks': [
			{'Authors': ['Carl II', 'Dan']},
			{'Authors': ['Alex', 'Yngwie', 'Zack']},
			{'Authors': ['Rob']}
		]})

	def testToEmptyList(self):
		"""Check that insertion to non-existing list creates this list"""
		self.prepareStandSimpleList()

		self.conn.insertMany(self.id2,
			['tracks', 2, 'Authors', None],
			['Earl', 'Fred'])

		res = self.conn.readByMask(self.id2, ['tracks', None, 'Authors', None])
		self.assertEqual(res, {'tracks': [
			None,
			None,
			{'Authors': ['Earl', 'Fred']}
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
			'Pikeman', 'Archer','Monk', 2,
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


def get_class():
	return Insert
