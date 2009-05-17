"""Unit tests for database layer"""

import unittest

import testhelpers
import database
from interfaces import *

def _compareLists(l1, l2):
	"""Check if all elements of the first list exist in the second list"""
	for elem in l1:
		if elem in l2:
			l2.remove(elem)
		else:
			raise Exception("Cannot find " + str(elem) + " in second list")

def _getParameterized(base_class, name_prefix, db_class, db_file_name):
	"""Get named test suite with predefined setUp()"""
	class Derived(base_class):
		def setUp(self):
			self.db = db_class(db_file_name)

	Derived.__name__ = name_prefix + "." + base_class.__name__

	return Derived

class TestRequest(unittest.TestCase):
	"""Base class for database requests testing"""

	def setUp(self):
		"""Stub for setUp() so that nobody creates the instance of this class"""
		raise Exception("Not implemented")

	def checkRequestResult(self, res, expected):
		"""Compare request results with expected list"""
		self.failUnless(isinstance(res, list), "Request result has type " + str(type(res)))
		self.failUnless(len(res) == len(expected), "Request returned " + str(len(res)) + " results")
		_compareLists(res, expected)

	def addObject(self, id, fields={}):
		"""Add object with given fields to database"""
		field_objs = [Field(key, 'text', fields[key]) for key in fields.keys()]
		self.db.processRequest(ModifyRequest(id, field_objs))

	def prepareStandNoList(self):
		"""Prepare DB wiht several objects which contain only hashes"""
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})
		self.addObject('3', {'name': 'Carl', 'phone': '3333', 'age': '27'})
		self.addObject('4', {'name': 'Don', 'phone': '4444', 'age': '20'})
		self.addObject('5', {'name': 'Alex', 'phone': '1111', 'age': '22'})

	def prepareStandSimpleList(self):
		"""Prepare DB with several objects which contain simple lists"""
		self.db.processRequest(ModifyRequest('1', [
			Field(['tracks', 0], value='Track 1'),
			Field(['tracks', 1], value='Track 2'),
			Field(['tracks', 2], value='Track 3')]
			))
		self.db.processRequest(ModifyRequest('2', [
			Field(['tracks', 0], value='Track 2'),
			Field(['tracks', 1], value='Track 1')]
			))

	def prepareStandNestedList(self):
		"""Prepare DB with several objects which contain nested lists"""
		self.db.processRequest(ModifyRequest('1', [
			Field(['tracks', 0], value='Track 1'),
			Field(['tracks', 0, 'Name'], value='Track 1 name'),
			Field(['tracks', 0, 'Length'], value='Track 1 length'),
			Field(['tracks', 0, 'Authors', 0], value='Alex'),
			Field(['tracks', 0, 'Authors', 1], value='Bob'),

			Field(['tracks', 1], value='Track 2'),
			Field(['tracks', 1, 'Name'], value='Track 2 name'),
			Field(['tracks', 1, 'Authors', 0], value='Carl I')
			]))

		self.db.processRequest(ModifyRequest('2', [
			Field(['tracks', 0], value='Track 11'),
			Field(['tracks', 0, 'Name'], value='Track 1 name'),
			Field(['tracks', 0, 'Length'], value='Track 1 length'),
			Field(['tracks', 0, 'Authors', 0], value='Carl II'),
			Field(['tracks', 0, 'Authors', 1], value='Dan'),

			Field(['tracks', 1], value='Track 2'),
			Field(['tracks', 1, 'Name'], value='Track 2 name'),
			Field(['tracks', 1, 'Authors', 0], value='Alex')
			]))

class TestModifyRequest(TestRequest):
	"""Test different uses of ModifyRequest"""

	def testBlankObjectAddition(self):
		"""Check that object without fields can be created"""
		self.addObject('1')

	def testAdditionNoCheck(self):
		"""Check simple object addition"""
		self.prepareStandNoList()

	def testAddition(self):
		"""Simple object addition with result checking"""
		self.prepareStandNoList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '1111')))

		self.checkRequestResult(res, ['1', '5'])

	def testAdditionSameFields(self):
		"""Check that several equal fields are handled correctly"""
		self.db.processRequest(ModifyRequest('1', [
			Field(['tracks'], value='Track 1'),
			Field(['tracks'], value='Track 2'),
			Field(['tracks'], value='Track 3')]
			))

		res = self.db.processRequest(ReadRequest('1', [Field(['tracks'])]))

		# only last field value should be saved
		self.checkRequestResult(res, [
			Field(['tracks'], 'text', 'Track 3')
			])

	def testModificationNoCheck(self):
		"""Check object modification"""
		self.prepareStandNoList()

		self.db.processRequest(ModifyRequest('1', [
			Field('name', 'text', 'Zack')
		]))

	def testModification(self):
		"""Object modification with results checking"""
		self.prepareStandNoList()

		self.db.processRequest(ModifyRequest('1', [
			Field('name', 'text', 'Zack')
		]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('name'), SearchRequest.Eq(), 'Zack')))

		self.checkRequestResult(res, ['1'])

	def testModificationAddsField(self):
		"""Check that object modification can add a new field"""
		self.prepareStandNoList()

		self.db.processRequest(ModifyRequest('1', [
			Field('age', 'text', '66')
		]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('age'), SearchRequest.Eq(), '66')))

		self.checkRequestResult(res, ['1'])

	def testModificationAddsFieldTwice(self):
		"""Regression test for non-updating specification"""
		self.prepareStandNoList()

		# Add new field to object
		self.db.processRequest(ModifyRequest('1', [
			Field('age', 'text', '66')
		]))

		# Delete object. If specification was not updated,
		# new field is still in database
		self.db.processRequest(DeleteRequest('1'))

		# Add object again
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})

		# Check that field from old object is not there
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('age'), SearchRequest.Eq(), '66')))

		self.checkRequestResult(res, [])

	def testModificationPreservesFields(self):
		"""Check that modification preserves existing fields"""
		self.prepareStandNoList()

		self.db.processRequest(ModifyRequest('2', [
			Field('name', 'text', 'Zack')
		]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '2222')))

		self.checkRequestResult(res, ['2'])

class TestSearchRequest(TestRequest):
	"""Test operation of SearchRequest"""

	def testConditionAnd(self):
		"""Check complex condition with And operator"""
		self.prepareStandNoList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '1111'
			),
		SearchRequest.And(),
		SearchRequest.Condition(
			Field('age'), SearchRequest.Eq(), '22'
			)
		)))

		self.checkRequestResult(res, ['5'])

	def testConditionOr(self):
		"""Check complex condition with Or operator"""
		self.prepareStandNoList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '2222'
			),
		SearchRequest.Or(),
		SearchRequest.Condition(
			Field('age'), SearchRequest.Eq(), '27'
			)
		)))

		self.checkRequestResult(res, ['2', '3'])

	def testConditionInvert(self):
		"""Check operation of inversion flag in condition"""
		self.prepareStandNoList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '1111'
			),
		SearchRequest.And(),
		SearchRequest.Condition(
			Field('age'), SearchRequest.Eq(), '22', invert=True
			)
		)))

		self.checkRequestResult(res, ['1'])

	def testConditionInvertInRoot(self):
		"""Check if inversion flag works in the root of condition"""
		self.prepareStandNoList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '1111'
			),
		SearchRequest.And(),
		SearchRequest.Condition(
			Field('age'), SearchRequest.Eq(), '22', invert=True
			),
		invert=True
		)))

		self.checkRequestResult(res, ['2', '3', '4', '5'])

	def testConditionRegexp(self):
		"""Check operation of regexp-based condition"""
		self.prepareStandNoList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.Regexp(), '\d+'
			),
		SearchRequest.And(),
		SearchRequest.Condition(
			Field('age'), SearchRequest.Eq(), '22', invert=True
			)
		)))

		self.checkRequestResult(res, ['1', '2', '3', '4'])

	def testConditionRegexpOnPart(self):
		"""Regression for bug when there was match() instead of search() in regexp callback"""
		self.prepareStandNoList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('name'), SearchRequest.Regexp(), 'ex'
			)))

		self.checkRequestResult(res, ['1', '5'])

	def testNonExistentFieldInAndCondition(self):
		"""Check that condition on non-existent field works with And operator"""
		self.prepareStandNoList()

		# Second part of the condition should return empty list,
		# so the whole result should be empty too
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '1111'
			),
		SearchRequest.And(),
		SearchRequest.Condition(
			Field('blablabla'), SearchRequest.Eq(), '22', invert=True
			)
		)))

		self.checkRequestResult(res, [])

	def testNonExistentFieldInOrCondition(self):
		"""Check that condition on non-existent field works with Or operator"""
		self.prepareStandNoList()

		# Second part of the condition should return empty list,
		# so the whole result should be equal to the result of the first part
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '1111'
			),
		SearchRequest.Or(),
		SearchRequest.Condition(
			Field('blablabla'), SearchRequest.Eq(), '22', invert=True
			)
		)))

		self.checkRequestResult(res, ['1', '5'])

	def testListOneLevel(self):
		"""Check searching in simple lists"""
		self.prepareStandSimpleList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', None]), SearchRequest.Eq(), 'Track 2'
			)))

		self.checkRequestResult(res, ['1', '2'])

	def testListOneLevelDefinedPosition(self):
		"""Check search when some of positions in lists are defined"""
		self.prepareStandSimpleList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', 1]), SearchRequest.Eq(), 'Track 2'
			)))

		self.checkRequestResult(res, ['1'])

	def testNestedList(self):
		"""Check searching in nested lists"""
		self.prepareStandNestedList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', 0, 'Authors', None]), SearchRequest.Eq(), 'Alex'
			)))

		self.checkRequestResult(res, ['1'])

	def testNestedListRegexp(self):
		"""Check regexp searching in nested lists"""
		self.prepareStandNestedList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', 1, 'Authors', None]), SearchRequest.Regexp(), 'Carl'
			)))

		self.checkRequestResult(res, ['1'])

	def testNestedListComplexCondition(self):
		"""Check search in nested list with complex condition"""
		self.prepareStandNestedList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field(['tracks', None]), SearchRequest.Eq(), 'Track 11'
			),
		SearchRequest.And(),
		SearchRequest.Condition(
			Field(['tracks', None, 'Name']), SearchRequest.Regexp(), 'name'
			)
		)))

		self.checkRequestResult(res, ['2'])

class TestDeleteRequest(TestRequest):
	"""Test operation of DeleteRequest"""

	def testWholeObject(self):
		"""Check deletion of the whole object"""
		self.prepareStandNoList()

		self.db.processRequest(DeleteRequest('3'))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '3333')))
		self.checkRequestResult(res, [])

	def testWholeObjectPreservesOthers(self):
		"""Check that deletion of the whole object does not spoil the database"""
		self.prepareStandNoList()

		self.db.processRequest(DeleteRequest('3'))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '1111')))
		self.checkRequestResult(res, ['1', '5'])

	def testExistentFieldsPreservesOtherFiels(self):
		"""Check that deletion of existent fields preserves other object fields"""
		self.prepareStandNoList()

		self.db.processRequest(DeleteRequest('3', [Field('age'), Field('phone')]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('name'), SearchRequest.Eq(), 'Carl')))
		self.checkRequestResult(res, ['3'])

	def testExistentFieldsReallyDeleted(self):
		"""Check that deletion of existent fields actually deletes them"""
		self.prepareStandNoList()

		self.db.processRequest(DeleteRequest('3', [Field('age'), Field('phone')]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '3333')))
		self.checkRequestResult(res, [])

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('age'), SearchRequest.Eq(), '27')))
		self.checkRequestResult(res, [])

	def testNonExistentFields(self):
		"""Check deletion of non-existent fields"""
		self.prepareStandNoList()

		self.db.processRequest(DeleteRequest('2', [Field('name'), Field('blablabla')]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('name'), SearchRequest.Eq(), 'Bob')))
		self.checkRequestResult(res, [])

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '2222')))
		self.checkRequestResult(res, ['2'])

	def testAllElements(self):
		"""Test that deleting all elements does not spoil the database"""
		self.prepareStandNoList()

		# Remove all
		self.db.processRequest(DeleteRequest('1'))
		self.db.processRequest(DeleteRequest('2'))
		self.db.processRequest(DeleteRequest('3'))

		# Add object again
		self.addObject('2', {'name': 'Alex', 'phone': '2222'})

		# Check that addition was successful
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '2222')))

		self.checkRequestResult(res, ['2'])

	def testSimpleList(self):
		"""Test deletion from list"""
		self.prepareStandSimpleList()

		self.db.processRequest(DeleteRequest('1', [
			Field(['tracks', 1])
		]))

		res = self.db.processRequest(ReadRequest('1', [Field(['tracks', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0], 'text', 'Track 1'),
			Field(['tracks', 1], 'text', 'Track 3')
			])

class TestReadRequest(TestRequest):
	"""Test operation of ReadRequest"""

	def testAllFields(self):
		"""Check the operation of whole object reading"""
		self.prepareStandNoList()

		res = self.db.processRequest(ReadRequest('1'))
		self.checkRequestResult(res, [
			Field('name', 'text', 'Alex'),
			Field('phone', 'text', '1111')])

	def testSomeFields(self):
		"""Check the operation of reading some chosen fields"""
		self.prepareStandNoList()

		res = self.db.processRequest(ReadRequest('1', [Field('name')]))
		self.checkRequestResult(res, [
			Field('name', 'text', 'Alex'),
			])

	def testNonExistingField(self):
		"""Check that non-existent field is ignored during read"""
		self.prepareStandNoList()

		res = self.db.processRequest(ReadRequest('1', [Field('name'), Field('age')]))
		self.checkRequestResult(res, [
			Field('name', 'text', 'Alex')
			])

	def testAddedList(self):
		"""Check that list values can be read"""
		self.prepareStandSimpleList()

		res = self.db.processRequest(ReadRequest('1', [Field(['tracks', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0], 'text', 'Track 1'),
			Field(['tracks', 1], 'text', 'Track 2'),
			Field(['tracks', 2], 'text', 'Track 3')
			])

	def testAddedListComplexCondition(self):
		"""Check that read request works properly when some list positions are defined"""
		self.prepareStandNestedList()

		res = self.db.processRequest(ReadRequest('1', [Field(['tracks', None, 'Authors', 0])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Authors', 0], 'text', 'Alex'),
			Field(['tracks', 1, 'Authors', 0], 'text', 'Carl I')
			])

	def testFromMiddleLevelList(self):
		"""Check that one can read from list in the middle of the hierarchy"""
		self.prepareStandNestedList()

		res = self.db.processRequest(ReadRequest('1', [Field(['tracks', None, 'Name'])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Name'], 'text', 'Track 1 name'),
			Field(['tracks', 1, 'Name'], 'text', 'Track 2 name')
			])


def suite():
	"""Generate test suite for this module"""
	res = testhelpers.NamedTestSuite()

	parameters = [
		('memory.sqlite3', database.Sqlite3Database, ':memory:'),
	]

	requests = [TestModifyRequest, TestSearchRequest, TestDeleteRequest, TestReadRequest]

	for parameter in parameters:
		for request in requests:
			res.addTest(unittest.TestLoader().loadTestsFromTestCase(
				_getParameterized(request, *parameter)))

	return res

if __name__ == '__main__':
	testhelpers.TextTestRunner(verbosity=2).run(suite())
