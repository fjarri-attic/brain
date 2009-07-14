"""Unit tests for database layer insert request"""

import unittest

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import brain
import brain.op as op
from functionality.requests import TestRequest

class Insert(TestRequest):
	"""Test operation of InsertRequest"""

	def testToTheMiddleSimpleList(self):
		"""Check insertion to the middle of simple list"""
		self.prepareStandSimpleList()
		self.conn.insertMany(self.id1, ['tracks', 1], ['Track 4', 'Track 5'])
		res = self.conn.read(self.id1, ['tracks', None])
		self.assertEqual(res, {'tracks': [
			'Track 1', 'Track 4', 'Track 5', 'Track 2', 'Track 3'
		]})

	def testToTheBeginningSimpleList(self):
		"""Check insertion to the beginning of simple list"""
		self.prepareStandSimpleList()
		self.conn.insertMany(self.id1, ['tracks', 0], ['Track 4', 'Track 5'])
		res = self.conn.read(self.id1, ['tracks', None])
		self.assertEqual(res, {'tracks': [
			'Track 4', 'Track 5', 'Track 1', 'Track 2', 'Track 3'
		]})

	def testToTheEndSimpleList(self):
		"""Check insertion to the end of simple list"""
		self.prepareStandSimpleList()
		self.conn.insertMany(self.id1, ['tracks', None], ['Track 4', 'Track 5'])
		res = self.conn.read(self.id1, ['tracks', None])
		self.assertEqual(res, {'tracks': [
			'Track 1', 'Track 2', 'Track 3', 'Track 4', 'Track 5'
		]})

	def testToTheMiddleNestedList(self):
		"""Test insertion to the middle of nested list"""
		self.prepareStandNestedList()
		self.conn.insertMany(self.id2, ['tracks', 1], [
			{'Name': 'Track 4 name'}, {'Name': 'Track 5 name'}
		])

		res = self.conn.read(self.id2, ['tracks', None, 'Name'])
		self.assertEqual(res, {'tracks': [
			{'Name': 'Track 1 name'},
			{'Name': 'Track 4 name'},
			{'Name': 'Track 5 name'},
			{'Name': 'Track 2 name'},
			{'Name': 'Track 3 name'}
		]})

		res = self.conn.read(self.id2, ['tracks', None, 'Authors', None])
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

		res = self.conn.read(self.id2, ['tracks', None, 'Name'])
		self.assertEqual(res, {'tracks': [
			{'Name': 'Track 4 name'},
			{'Name': 'Track 5 name'},
			{'Name': 'Track 1 name'},
			{'Name': 'Track 2 name'},
			{'Name': 'Track 3 name'}
		]})

		res = self.conn.read(self.id2, ['tracks', None, 'Authors', None])
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

		res = self.conn.read(self.id2, ['tracks', None, 'Name'])
		self.assertEqual(res, {'tracks': [
			{'Name': 'Track 1 name'},
			{'Name': 'Track 2 name'},
			{'Name': 'Track 3 name'},
			{'Name': 'Track 4 name'},
			{'Name': 'Track 5 name'}
		]})

		res = self.conn.read(self.id2, ['tracks', None, 'Authors', None])
		self.assertEqual(res, {'tracks': [
			{'Authors': ['Carl II', 'Dan']},
			{'Authors': ['Alex']},
			{'Authors': ['Rob']},
		]})

	def testTreeToTheBeginningNestedList(self):
		"""Test insertion of the data tree to the beginning of nested list"""
		self.prepareStandNestedList()
		self.conn.insert(self.id2, ['tracks', 0], {'Authors': ['Earl', 'Fred']})
		res = self.conn.read(self.id2, ['tracks', None, 'Name'])
		self.assertEqual(res, {'tracks': [
			None,
			{'Name': 'Track 1 name'},
			{'Name': 'Track 2 name'},
			{'Name': 'Track 3 name'}
		]})

		res = self.conn.read(self.id2, ['tracks', None, 'Authors', None])
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
		res = self.conn.read(self.id2, ['tracks', None, 'Name'])
		self.assertEqual(res, {'tracks': [
			{'Name': 'Track 1 name'},
			{'Name': 'Track 2 name'},
			{'Name': 'Track 3 name'}
		]})

		res = self.conn.read(self.id2, ['tracks', None, 'Authors', None])
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

		res = self.conn.read(self.id2, ['tracks', None, 'Authors', None])
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

		res = self.conn.read(self.id2, ['tracks', None, 'Authors', None])
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
		res = self.conn.read(self.id1, ['meta', None])
		self.assertEqual(res, {'meta': [
			'Pikeman', 'Archer','Monk', 2,
			10.0, 1, 2, 4.0, 5.0, b'Gryphon', b'Swordsman'
		]})

def get_class():
	return Insert
