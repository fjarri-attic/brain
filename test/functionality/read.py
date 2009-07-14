"""Unit tests for database layer read request"""

import unittest

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import brain
from functionality.requests import TestRequest

class Read(TestRequest):
	"""Test operation of ReadRequest"""

	def testAllFields(self):
		"""Check the operation of whole object reading"""
		self.prepareStandNoList()
		res = self.conn.read(self.id1)
		self.assertEqual(res, {'name': 'Alex', 'phone': '1111'})

	def testSomeFields(self):
		"""Check the operation of reading some chosen fields"""
		self.prepareStandNoList()
		res = self.conn.read(self.id1, ['name'])
		self.assertEqual(res, {'name': 'Alex'})

	def testNonExistingField(self):
		"""Check that non-existent field is ignored during read"""
		self.prepareStandNoList()
		res = self.conn.read(self.id1, ['age'])
		self.assertEqual(res, [])

	def testAddedList(self):
		"""Check that list values can be read"""
		self.prepareStandSimpleList()
		res = self.conn.read(self.id1, ['tracks', None])
		self.assertEqual(res, {'tracks': ['Track 1', 'Track 2', 'Track 3']})

	def testAddedListComplexCondition(self):
		"""Check that read request works properly when some list positions are defined"""
		self.prepareStandNestedList()
		res = self.conn.read(self.id1, ['tracks', None, 'Authors', 0])
		self.assertEqual(res, {'tracks': [
			{'Authors': ['Alex']},
			{'Authors': ['Carl I']}
		]})

	def testFromMiddleLevelList(self):
		"""Check that one can read from list in the middle of the hierarchy"""
		self.prepareStandNestedList()
		res = self.conn.read(self.id1, ['tracks', None, 'Name'])
		self.assertEqual(res, {'tracks': [
			{'Name': 'Track 1 name'},
			{'Name': 'Track 2 name'}
		]})

	def testNonExistingObject(self):
		"""Check that read request for non-existing object raises error"""
		self.prepareStandNoList()
		self.conn.delete(self.id1)
		self.failUnlessRaises(brain.LogicError, self.conn.read, self.id1)

	def testSeveralTypesAtOnce(self):
		"""Check that different values of types can be read at once by mask"""
		self.prepareStandDifferentTypes()
		res = self.conn.read(self.id1, ['meta', None])
		self.assertEqual(res, {'meta': [
			'Pikeman', 'Archer', 1, 2, 4.0, 5.0, b'Gryphon', b'Swordsman'
		]})

	def testSubTree(self):
		"""Check that subtree can be read at once"""
		self.prepareStandNestedList()
		res = self.conn.read(self.id1, ['tracks', 0, 'Authors'])
		self.assertEqual(res, {'tracks': [
			{'Authors': ['Alex', 'Bob']}
		]})

	def testSubTreesByMask(self):
		"""Check that several subtrees can be read at once by mask"""
		self.prepareStandNestedList()
		res = self.conn.read(self.id1, ['tracks', None, 'Authors'])
		self.assertEqual(res, {'tracks': [
			{'Authors': ['Alex', 'Bob']},
			{'Authors': ['Carl I']}
		]})

	def testComplexStructure(self):
		"""Check that complex structure can be read at once"""
		self.prepareStandDifferentTypes()
		res = self.conn.read(self.id1, ['tracks'])
		self.assertEqual(res, {'tracks': [
			{'Name': 'Track 1 name', 'Length': 300, 'Volume': 29.4,
				'Authors': ['Alex', 'Bob'],
				'Data': b'\x00\x01\x02'},
			{'Name': 'Track 2 name', 'Length': 350.0, 'Volume': 26,
				'Authors': ['Carl', 'Dan'],
				'Rating': 4,
				'Data': b'\x00\x01\x03'}]})

	def testSeveralComplexStructures(self):
		"""Check that several complex structures can be read at once"""
		self.prepareStandDifferentTypes()
		res = self.conn.readMany(self.id1, [['tracks', None, 'Name'],
			['tracks', None, 'Authors', None]])
		self.assertEqual(res, {'tracks': [
			{'Name': 'Track 1 name', 'Authors': ['Alex', 'Bob']},
			{'Name': 'Track 2 name', 'Authors': ['Carl', 'Dan']}
		]})


def get_class():
	return Read
