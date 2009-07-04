"""Unit tests for database layer modify request"""

import unittest

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import brain
import brain.op as op
from test.functionality.requests import TestRequest

class Modify(TestRequest):
	"""Test different uses of ModifyRequest"""

	@unittest.skip("Not implemented yet")
	def testBlankObjectAddition(self):
		"""Check that object without fields cannot be created"""

		# Value is the mandatory paramter to this function
		self.assertRaises(TypeError, self.conn.create)

		self.assertRaises(brain.FacadeError, self.conn.create, {})
		self.assertRaises(brain.FacadeError, self.conn.create, [])
		self.assertRaises(brain.FacadeError, self.conn.create, None)

	def testModifyNothing(self):
		"""Check that modification without parameters does nothing"""
		orig_data = {'fld': 1}
		obj = self.conn.create(orig_data)
		self.conn.modify(obj, None)
		data = self.conn.read(obj)
		self.assertEqual(data, orig_data)

	def testAdditionNoCheck(self):
		"""Check simple object addition"""
		self.prepareStandNoList()

	def testAddition(self):
		"""Simple object addition with result checking"""
		self.prepareStandNoList()
		res = self.conn.search(['phone'], op.EQ, '1111')
		self.assertSameElements(res, [self.id1, self.id5])

	def testModificationNoCheck(self):
		"""Check object modification"""
		self.prepareStandNoList()
		self.conn.modify(self.id1, 'Zack', ['name'])

	def testModification(self):
		"""Object modification with results checking"""
		self.prepareStandNoList()
		self.conn.modify(self.id1, 'Zack', ['name'])
		res = self.conn.search(['name'], op.EQ, 'Zack')
		self.assertEqual(res, [self.id1])

	def testModificationAddsField(self):
		"""Check that object modification can add a new field"""
		self.prepareStandNoList()
		self.conn.modify(self.id1, '66', ['age'])
		res = self.conn.search(['age'], op.EQ, '66')
		self.assertEqual(res, [self.id1])

	def testModificationAddsFieldTwice(self):
		"""Regression test for non-updating specification"""
		self.prepareStandNoList()

		# Add new field to object
		self.conn.modify(self.id1, '66', ['age'])

		# Delete object. If specification was not updated,
		# new field is still in database
		self.conn.delete(self.id1)

		# Add object again
		self.id1 = self.conn.create({'name': 'Alex', 'phone': '1111'})

		# Check that field from old object is not there
		res = self.conn.search(['age'], op.EQ, '66')
		self.assertEqual(res, [])

	def testModificationPreservesFields(self):
		"""Check that modification preserves existing fields"""
		self.prepareStandNoList()
		self.conn.modify(self.id2, 'Zack', ['name'])
		res = self.conn.search(['phone'], op.EQ, '2222')
		self.assertEqual(res, [self.id2])

	def testListAdditions(self):
		"""Regression test for erroneous modify results for lists"""
		self.prepareStandSimpleList()
		self.conn.modify(self.id1, 'Track 4', ['tracks', 3])
		self.conn.modify(self.id1, 'Track 5', ['tracks', 4])
		res = self.conn.read(self.id1, ['tracks', None])
		self.assertEqual(res, {'tracks': [
			'Track 1', 'Track 2', 'Track 3', 'Track 4', 'Track 5']})

	def testModificationAddsList(self):
		"""Check that modification request creates necessary hierarchy"""
		self.prepareStandNestedList()
		self.conn.modify(self.id1, 'Blablabla', ['tracks', 2, 'Lyrics', 0])

	def testListOnTopOfMap(self):
		"""Check that list cannot be created if map exists on the same level"""
		self.prepareStandNestedList()
		self.assertRaises(brain.StructureError, self.conn.modify,
			self.id1, 'Blablabla', ['tracks', 2, 0])

	def testMapOnTopOfList(self):
		"""Check that map cannot be created if list exists on the same level"""
		self.prepareStandNestedList()
		self.assertRaises(brain.StructureError, self.conn.modify,
			self.id1, 'Blablabla', ['tracks', 'some_name'])

	def testModificationAddsNewField(self):
		"""Check that modification can add totally new field to object"""
		self.prepareStandNoList()
		self.conn.modify(self.id1, 'Mr', ['title'])
		res = self.conn.search(['title'], op.EQ, 'Mr')
		self.assertEqual(res, [self.id1])

	def testAdditionDifferentTypes(self):
		"""Test that values of different types can be added"""
		values = ['text value', 123, 45.56, b'\x00\x01']

		# create fields with values of different types
		reference_data = {('fld' + str(i)): x for i, x in enumerate(values)}

		# check that all of them can be added and read
		obj = self.conn.create(reference_data)
		res = self.conn.read(obj)
		self.assertEqual(res, reference_data)

	def testModificationChangesFieldType(self):
		"""Test that you can change type of field value"""
		values = ['text value', 123, 45.56, b'\x00\x01']

		# create fields with values of different types
		obj = None
		fld = 'fld'
		for value in values:
			if obj is None:
				obj = self.conn.create(value, [fld])
			else:
				self.conn.modify(obj, value, [fld])

			res = self.conn.read(obj)
			self.assertEqual(res, {fld: value})

	def testSeveralTypesInOneField(self):
		"""
		Check that different objects can store values
		of different types in the same field
		"""
		fld = 'fld'
		objects = [
			{fld: 1}, {fld: 'text'}, {fld: 1.234}, {fld: b'\x00\x01'}
		]

		# create objects
		ids_and_data = [(self.conn.create(data), data) for data in objects]

		# check that objects can be read
		for id, data in ids_and_data:
			res = self.conn.read(id)
			self.assertEqual(res, data)

	def testSeveralTypesInList(self):
		"""Check that list can store values of different types"""
		data = {'vals': ['Zack', 1, 1.234, b'Zack']}
		obj = self.conn.create(data)
		res = self.conn.read(obj)
		self.assertEqual(res, data)

	def testMapOnTopOfMapValue(self):
		"""Check that map can be written on top of existing value"""
		fld = 'fld1'
		data = {'fld2': {'fld3': 2, 'fld4': 'a'}}

		obj = self.conn.create({fld: 'val1'})
		self.conn.modify(obj, data, [fld])
		res = self.conn.read(obj)
		self.assertEqual(res, {fld: data})

	def testMapOnTopOfListElement(self):
		"""Check that map can be written on top of existing list element"""
		fld = 'fld1'
		data = {'fld3': 2}

		obj = self.conn.create({fld: ['val1', 'val2']})
		self.conn.modify(obj, data, [fld, 1])
		res = self.conn.read(obj)
		self.assertEqual(res, {fld: ['val1', data]})

	def testListOnTopOfListElement(self):
		"""Check that list can be written on top of existing list element"""
		fld = 'fld1'
		data = [2]

		obj = self.conn.create({fld: ['val1', 'val2']})
		self.conn.modify(obj, data, [fld, 1])
		res = self.conn.read(obj)
		self.assertEqual(res, {fld: ['val1', data]})

	def testNoneValue(self):
		"""Check basic support of Null values"""
		data = {'fld1': [None, 1]}
		obj = self.conn.create(data)
		res = self.conn.read(obj)
		self.assertEqual(res, data)

	def testChangeListElementType(self):
		"""
		Regression test, showing that it is necessary to check all possible
		value types when modifying value in list
		"""
		obj = self.conn.create({'fld1': [1, 'a']})
		self.conn.modify(obj, 2, ['fld1', 1])
		res = self.conn.read(obj)
		self.assertEqual(res, {'fld1': [1, 2]})

	def testObjectCreation(self):
		"""Check that passing None to modify() creates new element"""
		data = {'fld1': [1]}
		obj = self.conn.modify(None, data)
		res = self.conn.read(obj)
		self.assertEqual(res, data)

def get_class():
	return Modify
