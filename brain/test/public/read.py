"""Unit tests for database layer read request"""

import unittest

import brain

import helpers
from public.requests import TestRequest, getParameterized

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
		res = self.conn.readByMask(self.id1, ['name'])
		self.assertEqual(res, {'name': 'Alex'})

	def testNonExistingField(self):
		"""Check that non-existent field is ignored during read"""
		self.prepareStandNoList()
		self.assertRaises(brain.LogicError, self.conn.readByMask, self.id1, ['age'])

	def testAddedList(self):
		"""Check that list values can be read"""
		self.prepareStandSimpleList()
		res = self.conn.readByMask(self.id1, ['tracks', None])
		self.assertEqual(res, {'tracks': ['Track 1', 'Track 2', 'Track 3']})

	def testAddedListComplexCondition(self):
		"""Check that read request works properly when some list positions are defined"""
		self.prepareStandNestedList()
		res = self.conn.readByMask(self.id1, ['tracks', None, 'authors', 0])
		self.assertEqual(res, {'tracks': [
			{'authors': ['Alex']},
			{'authors': ['Carl I']}
		]})

	def testFromMiddleLevelList(self):
		"""Check that one can read from list in the middle of the hierarchy"""
		self.prepareStandNestedList()
		res = self.conn.readByMask(self.id1, ['tracks', None, 'name'])
		self.assertEqual(res, {'tracks': [
			{'name': 'Track 1 name'},
			{'name': 'Track 2 name'}
		]})

	def testNonExistingObject(self):
		"""Check that read request for non-existing object raises error"""
		self.prepareStandNoList()
		self.conn.delete(self.id1)
		self.failUnlessRaises(brain.LogicError, self.conn.read, self.id1)

	def testSeveralTypesAtOnce(self):
		"""Check that different values of types can be read at once by mask"""
		self.prepareStandDifferentTypes()
		res = self.conn.readByMask(self.id1, ['meta', None])
		self.assertEqual(res, {'meta': [
			'Pikeman', 'Archer', 1, 2, 4.0, 5.0, b'Gryphon', b'Swordsman'
		]})

	def testSubTree(self):
		"""Check that subtree can be read at once"""
		self.prepareStandNestedList()
		res = self.conn.readByMask(self.id1, ['tracks', 0, 'authors'])
		self.assertEqual(res, {'tracks': [
			{'authors': ['Alex', 'Bob']}
		]})

	def testSubTreesByMask(self):
		"""Check that several subtrees can be read at once by mask"""
		self.prepareStandNestedList()
		res = self.conn.readByMask(self.id1, ['tracks', None, 'authors'])
		self.assertEqual(res, {'tracks': [
			{'authors': ['Alex', 'Bob']},
			{'authors': ['Carl I']}
		]})

	def testComplexStructure(self):
		"""Check that complex structure can be read at once"""
		self.prepareStandDifferentTypes()
		res = self.conn.readByMask(self.id1, ['tracks'])
		self.assertEqual(res, {'tracks': [
			{'name': 'Track 1 name', 'length': 300, 'volume': 29.4,
				'authors': ['Alex', 'Bob'],
				'data': b'\x00\x01\x02'},
			{'name': 'Track 2 name', 'length': 350.0, 'volume': 26,
				'authors': ['Carl', 'Dan'],
				'rating': 4,
				'data': b'\x00\x01\x03'}]})

	def testSeveralComplexStructures(self):
		"""Check that several complex structures can be read at once"""
		self.prepareStandDifferentTypes()
		res = self.conn.readByMasks(self.id1, [['tracks', None, 'name'],
			['tracks', None, 'authors', None]])
		self.assertEqual(res, {'tracks': [
			{'name': 'Track 1 name', 'authors': ['Alex', 'Bob']},
			{'name': 'Track 2 name', 'authors': ['Carl', 'Dan']}
		]})

	def testReadNullValueFromRoot(self):
		"""
		Regression test for incorrect getValue() logic:
		When reading NULL value which does not have list among its ancestors
		(and therefore has blank column condition), incorrect SQL query was formed
		"""
		data = {'none_field': None}
		obj = self.conn.create(data)
		res = self.conn.read(obj)
		self.assertEqual(res, data)

	def testReadFromPath(self):
		"""Check that structure can be read from path"""
		self.prepareStandDifferentTypes()
		res = self.conn.read(self.id1, ['tracks', 0, 'authors'])
		self.assertEqual(res, ['Alex', 'Bob'])

	def testReadFromPathByMask(self):
		"""Check that path and masks parameters work simultaneously"""
		self.prepareStandDifferentTypes()
		res = self.conn.read(self.id1, ['tracks'], [[None, 'authors']])
		self.assertEqual(res, [{'authors': ['Alex', 'Bob']}, {'authors': ['Carl', 'Dan']}])

	def testReadFromNonExistentPath(self):
		"""Check that attempt to read from non-existent path raises LogicError"""
		self.prepareStandNoList()
		self.assertRaises(brain.LogicError, self.conn.read,
			self.id1, ['blablabla'])

	def testReadFromNonExistentListElement(self):
		"""Check that attempt to read from non-existent list element raises LogicError"""
		obj = self.conn.create([1, [2, 3, 4]])
		self.assertRaises(brain.LogicError, self.conn.read, obj, [0, 0])

	def testReadPathAndMasks(self):
		"""Check that path and masks arguments ofr read() work simultaneously"""
		obj = self.conn.create({'tracks': [{'name': 'track 1', 'length': 240},
			{'name': 'track 2', 'length': 300}]})
		res = self.conn.read(obj, ['tracks'], [[None, 'length']])
		self.assertEqual(res, [{'length': 240}, {'length': 300}])


def suite(engine_params, connection_generator):
	res = helpers.NamedTestSuite('read')
	res.addTestCaseClass(getParameterized(Read, engine_params, connection_generator))
	return res
