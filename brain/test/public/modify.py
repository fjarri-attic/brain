"""Unit tests for database layer modify request"""

import unittest

import brain
import brain.op as op

import helpers
from public.requests import TestRequest, getParameterized

class Modify(TestRequest):
	"""Test different uses of ModifyRequest"""

	def testBlankObjectAddition(self):
		"""Check that object without fields can be created"""
		for data in [{}, [], None]:
			obj = self.conn.create(data)
			res = self.conn.read(obj)
			self.assertEqual(res, data)

	def testStoreNothing(self):
		"""Check that None or empty structure can be stored in object"""
		for data in [{}, [], None]:
			obj = self.conn.create({'fld': 1})
			self.conn.modify(obj, None, data)
			res = self.conn.read(obj)
			self.assertEqual(res, data)

	def testStoreInRoot(self):
		"""Check that values can be stored in root level of object"""
		for data in [1, 1.0, "aaa", b"aaa", None]:
			obj = self.conn.create(data)
			res = self.conn.read(obj)
			self.assertEqual(res, data)

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
		self.conn.modify(self.id1, ['name'], 'Zack')

	def testModification(self):
		"""Object modification with results checking"""
		self.prepareStandNoList()
		self.conn.modify(self.id1, ['name'], 'Zack')
		res = self.conn.search(['name'], op.EQ, 'Zack')
		self.assertEqual(res, [self.id1])

	def testModificationAddsField(self):
		"""Check that object modification can add a new field"""
		self.prepareStandNoList()
		self.conn.modify(self.id1, ['age'], '66')
		res = self.conn.search(['age'], op.EQ, '66')
		self.assertEqual(res, [self.id1])

	def testModificationAddsFieldTwice(self):
		"""Regression test for non-updating specification"""
		self.prepareStandNoList()

		# Add new field to object
		self.conn.modify(self.id1, ['age'], '66')

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
		self.conn.modify(self.id2, ['name'], 'Zack')
		res = self.conn.search(['phone'], op.EQ, '2222')
		self.assertEqual(res, [self.id2])

	def testListAdditions(self):
		"""Regression test for erroneous modify results for lists"""
		self.prepareStandSimpleList()
		self.conn.modify(self.id1, ['tracks', 3], 'Track 4')
		self.conn.modify(self.id1, ['tracks', 4], 'Track 5')
		res = self.conn.readByMask(self.id1, ['tracks', None])
		self.assertEqual(res, {'tracks': [
			'Track 1', 'Track 2', 'Track 3', 'Track 4', 'Track 5']})

	def testModificationAddsList(self):
		"""Check that modification request creates necessary hierarchy"""
		self.prepareStandNestedList()
		self.conn.modify(self.id1, ['tracks', 2, 'lyrics', 0], 'Blablabla')
		res = self.conn.read(self.id1, ['tracks', 2, 'lyrics'])
		self.assertEqual(res, ['Blablabla'])

	def testListOnTopOfMap(self):
		"""Check that list cannot be created if map exists on the same level"""
		self.prepareStandNestedList()
		self.assertRaises(brain.StructureError, self.conn.modify,
			self.id1, ['tracks', 2, 0], 'Blablabla')

	def testMapOnTopOfList(self):
		"""Check that map cannot be created if list exists on the same level"""
		self.prepareStandNestedList()
		self.assertRaises(brain.StructureError, self.conn.modify,
			self.id1, ['tracks', 'some_name'], 'Blablabla')

	def testListOnTopOfRootMap(self):
		"""Check that root list cannot be created if map exists on the same level"""
		obj = self.conn.create({'name': 'Alex', 'age': 22})
		self.assertRaises(brain.StructureError, self.conn.modify, obj, [0], 2)

	def testMapOnTopOfRootList(self):
		"""Check that root map cannot be created if list exists on the same level"""
		obj = self.conn.create(['abc', 'def'])
		self.assertRaises(brain.StructureError, self.conn.modify, obj, ['ghi'], 2)

	def testModificationAddsNewField(self):
		"""Check that modification can add totally new field to object"""
		self.prepareStandNoList()
		self.conn.modify(self.id1, ['title'], 'Mr')
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
				self.conn.modify(obj, [fld], value)

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
		self.conn.modify(obj, [fld], data)
		res = self.conn.read(obj)
		self.assertEqual(res, {fld: data})

	def testMapOnTopOfListElement(self):
		"""Check that map can be written on top of existing list element"""
		fld = 'fld1'
		data = {'fld3': 2}

		obj = self.conn.create({fld: ['val1', 'val2']})
		self.conn.modify(obj, [fld, 1], data)
		res = self.conn.read(obj)
		self.assertEqual(res, {fld: ['val1', data]})

	def testListOnTopOfListElement(self):
		"""Check that list can be written on top of existing list element"""
		fld = 'fld1'
		data = [2]

		obj = self.conn.create({fld: ['val1', 'val2']})
		self.conn.modify(obj, [fld, 1], data)
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
		self.conn.modify(obj, ['fld1', 1], 2)
		res = self.conn.read(obj)
		self.assertEqual(res, {'fld1': [1, 2]})

	def testListAndMapInList(self):
		"""
		Regression test for case when nested list and map as elements of the list
		caused false positive from data structure check
		"""
		data = {'root': [{'key': 2}, [1]]}
		obj = self.conn.create(data)
		res = self.conn.read(obj)
		self.assertEqual(res, data)

	def testOverwriteValueWithStructure(self):
		"""
		Regression test for saving structure to the place of existing list element
		It shows that it is necessary to check all ancestors when rewriting a value
		"""
		data = {'key': [1, 'elem1']}
		to_add = {'to_add': None}
		obj = self.conn.create(data)
		self.conn.modify(obj, ['key', 0], to_add)
		res = self.conn.read(obj)
		data['key'][0] = to_add
		self.assertEqual(res, data)

	def testRefcountForNullValue(self):
		"""
		Regression test for bug in refcounter logic, when refcounter for NULL values
		could not be increased because '=NULL' instead of 'ISNULL' was used in
		update query
		"""
		data = {'key': [[None], [77, None]]}
		to_add = 'aaa'
		obj = self.conn.create(data)
		self.conn.modify(obj, ['key', 1, 1], to_add)
		res = self.conn.read(obj)
		data['key'][1][1] = to_add
		self.assertEqual(res, data)

	def testOverwriteStructureWithValue(self):
		"""
		Regression test for saving value in place of structure - there was a bug in
		conficts removal logic, when child fields were not deleted on modification.
		"""
		obj = self.conn.create({'key': [['aaa']]})
		self.conn.modify(obj, ['key', 0], 'bbb')
		res = self.conn.read(obj)
		self.assertEqual(res, {'key': ['bbb']})

	def testOverwriteMapWithList(self):
		"""
		Regression test for bug with false positive in conflicts checker, when
		one creates list in place of existing map
		"""
		data = {'kkk': {'key2': 1}}
		to_add = ['list_val']
		obj = self.conn.create(data)
		self.conn.modify(obj, ['kkk'], to_add)
		data['kkk'] = to_add
		res = self.conn.read(obj)
		self.assertEqual(res, data)

	def testDeleteListsizes(self):
		"""
		Check that when list is deleted due to modification, corresponding
		listsizes table is removed too
		"""
		obj = self.conn.create({'aaa': [1, 2, 3]})

		# replace list by value
		self.conn.modify(obj, ['aaa'], 'bbb')

		# replace value by new list
		self.conn.modify(obj, ['aaa'], [1])

		# insert value to the end of the list; if information
		# about the original list was not removed from the database,
		# it will think that the length of the list is 3, and store
		# new value in the wrong position
		self.conn.insert(obj, ['aaa', None], 2)
		res = self.conn.read(obj)
		self.assertEqual(res, {'aaa': [1, 2]})

	def testDeleteDuringAutovivification(self):
		"""
		Check that old value is properly deleted after
		structure is saved on top of value
		"""
		obj = self.conn.create({'key': 1})
		self.conn.modify(obj, ['key', 1], 2, remove_conflicts=True)
		res = self.conn.read(obj)
		self.assertEqual(res, {'key': [None, 2]})

	def testAutovivificationCreatesHierarchy(self):
		"""
		Check that all necessary hierarchy (not just the ending leaf)
		is created during autovivification
		"""
		obj = self.conn.create({'key': 1})
		self.conn.modify(obj, ['key', 'key2', 'key3'], 3, remove_conflicts=True)
		res = self.conn.read(obj, ['key', 'key2'])
		self.assertEqual(res, {'key3': 3})

	def testAutoFillWithNones(self):
		"""Check that when modification creates new list elements, they are filled with Nones"""
		obj = self.conn.create([1])
		self.conn.modify(obj, [5], 1)
		res = self.conn.read(obj, [3])
		self.assertEqual(res, None)

	def testAutoFillWithNonesNestedList(self):
		"""
		Check that when modification creates new nested list elements,
		they are filled with Nones
		"""
		obj = self.conn.create({'a': 1})
		self.conn.modify(obj, [0, 2, 'key1', 'key2'], 3, remove_conflicts=True)
		res = self.conn.read(obj, [0, 0])
		self.assertEqual(res, None)


def suite(engine_params, connection_generator):
	res = helpers.NamedTestSuite('modify')
	res.addTestCaseClass(getParameterized(Modify, engine_params, connection_generator))
	return res
