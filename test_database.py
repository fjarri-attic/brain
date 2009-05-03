import unittest

import testhelpers
import database
from interfaces import *

def compareLists(l1, l2):
	for elem in l1:
		if elem in l2:
			l2.remove(elem)
		else:
			raise Exception("Cannot find " + str(elem) + " in second list")

class TestRequests(unittest.TestCase):

	def getParameterized(name_prefix, db_class, db_file_name):
		class Derived(TestRequests):
			def setUp(self):
				self.db = db_class(db_file_name)

		Derived.__name__ = name_prefix + "." + TestRequests.__name__

		return Derived

	def setUp(self):
		raise Exception("Not implemented")

	def checkSearchResult(self, res, expected):
		self.failUnless(isinstance(res, list), "Search result has type " + str(type(res)))
		self.failUnless(len(res) == len(expected), "Search returned " + str(len(res)) + " results")
		compareLists(res, expected)

	def checkReadResult(self, res, expected):
		self.failUnless(isinstance(res, list), "Read result has type " + str(type(res)))
		self.failUnless(len(res) == len(expected), "Read returned " + str(len(res)) + " results")
		compareLists(res, expected)

	def addObject(self, id, fields={}):
		field_objs = [Field(key, 'text', fields[key]) for key in fields.keys()]
		self.db.processRequest(ModifyRequest(id, field_objs))

	def testBlankObjectAddition(self):
		self.addObject('1')

	def testAdditionNoCheck(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})

	def testAddition(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '1111')))

		self.checkSearchResult(res, ['1'])

	def testModificationNoCheck(self):
		self.addObject('2', {'name': 'Alex', 'phone': '1111'})

		self.db.processRequest(ModifyRequest('2', [
			Field('name', 'text', 'Bob')
		]))

	def testModification(self):
		self.addObject('2', {'name': 'Alex', 'phone': '1111'})

		self.db.processRequest(ModifyRequest('2', [
			Field('name', 'text', 'Bob')
		]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('name'), SearchRequest.Eq(), 'Bob')))

		self.checkSearchResult(res, ['2'])

	def testModificationAddsField(self):
		self.addObject('2', {'name': 'Alex', 'phone': '1111'})

		self.db.processRequest(ModifyRequest('2', [
			Field('age', 'text', '27')
		]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('age'), SearchRequest.Eq(), '27')))

		self.checkSearchResult(res, ['2'])

	def testModificationAddFieldTwice(self):
		"""Regression test for non-updating specification"""

		# Add test object
		self.addObject('2', {'name': 'Alex', 'phone': '1111'})

		# Add new field to object
		self.db.processRequest(ModifyRequest('2', [
			Field('age', 'text', '27')
		]))

		# Delete object. If specification was not updated,
		# new field is still in database
		self.db.processRequest(DeleteRequest('2'))

		# Add object again
		self.addObject('2', {'name': 'Alex', 'phone': '1111'})

		# Check that field from old object is not there
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('age'), SearchRequest.Eq(), '27')))

		self.checkSearchResult(res, [])

	def testModificationPreservesFields(self):
		self.addObject('2', {'name': 'Alex', 'phone': '1111'})

		self.db.processRequest(ModifyRequest('2', [
			Field('name', 'text', 'Bob')
		]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '1111')))

		self.checkSearchResult(res, ['2'])

	def testMultipleModification(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})

		self.db.processRequest(ModifyRequest('2', [
			Field('name', 'text', 'Rob')
		]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('name'), SearchRequest.Eq(), 'Rob')))

		self.checkSearchResult(res, ['2'])

	def testSearchConditionAnd(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})
		self.addObject('3', {'name': 'Carl', 'phone': '3333', 'age': '27'})
		self.addObject('4', {'name': 'Don', 'phone': '4444', 'age': '20'})
		self.addObject('5', {'name': 'Alex', 'phone': '1111', 'age': '22'})

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '1111'
			),
		SearchRequest.And(),
		SearchRequest.Condition(
			Field('age'), SearchRequest.Eq(), '22'
			)
		)))

		self.checkSearchResult(res, ['5'])

	def testSearchConditionOr(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})
		self.addObject('3', {'name': 'Carl', 'phone': '3333', 'age': '27'})
		self.addObject('4', {'name': 'Don', 'phone': '4444', 'age': '20'})
		self.addObject('5', {'name': 'Alex', 'phone': '1111', 'age': '22'})

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '2222'
			),
		SearchRequest.Or(),
		SearchRequest.Condition(
			Field('age'), SearchRequest.Eq(), '27'
			)
		)))

		self.checkSearchResult(res, ['2', '3'])

	def testSearchConditionInvert(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})
		self.addObject('3', {'name': 'Carl', 'phone': '3333', 'age': '27'})
		self.addObject('4', {'name': 'Don', 'phone': '4444', 'age': '20'})
		self.addObject('5', {'name': 'Alex', 'phone': '1111', 'age': '22'})

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '1111'
			),
		SearchRequest.And(),
		SearchRequest.Condition(
			Field('age'), SearchRequest.Eq(), '22', invert=True
			)
		)))

		self.checkSearchResult(res, ['1'])

	def testSearchConditionInvertInRoot(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})
		self.addObject('3', {'name': 'Carl', 'phone': '3333', 'age': '27'})
		self.addObject('4', {'name': 'Don', 'phone': '4444', 'age': '20'})
		self.addObject('5', {'name': 'Alex', 'phone': '1111', 'age': '22'})

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

		self.checkSearchResult(res, ['2', '3', '4', '5'])

	def testSearchConditionRegexp(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})
		self.addObject('3', {'name': 'Carl', 'phone': '3333', 'age': '27'})
		self.addObject('4', {'name': 'Don', 'phone': '4444', 'age': '20'})
		self.addObject('5', {'name': 'Alex', 'phone': '1111', 'age': '22'})

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.Regexp(), '\d+'
			),
		SearchRequest.And(),
		SearchRequest.Condition(
			Field('age'), SearchRequest.Eq(), '22', invert=True
			)
		)))

		self.checkSearchResult(res, ['1', '2', '3', '4'])

	def testDeleteObject(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})
		self.addObject('3', {'name': 'Carl', 'phone': '3333', 'age': '27'})

		self.db.processRequest(DeleteRequest('3'))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '3333')))
		self.checkSearchResult(res, [])

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '1111')))
		self.checkSearchResult(res, ['1'])

	def testDeleteExistentFields(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})
		self.addObject('3', {'name': 'Carl', 'phone': '3333', 'age': '27'})

		self.db.processRequest(DeleteRequest('3', [Field('age'), Field('phone')]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('name'), SearchRequest.Eq(), 'Carl')))
		self.checkSearchResult(res, ['3'])

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '3333')))
		self.checkSearchResult(res, [])

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('age'), SearchRequest.Eq(), '27')))
		self.checkSearchResult(res, [])

	def testDeleteNonExistentFields(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})
		self.addObject('3', {'name': 'Carl', 'phone': '3333', 'age': '27'})

		self.db.processRequest(DeleteRequest('2', [Field('name'), Field('blablabla')]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('name'), SearchRequest.Eq(), 'Bob')))
		self.checkSearchResult(res, [])

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '2222')))
		self.checkSearchResult(res, ['2'])

	def testDeleteAllElements(self):
		"""Test that deleting all elements does not spoil the database"""

		# Add some objects
		self.addObject('2', {'name': 'Alex', 'phone': '1111'})
		self.addObject('3', {'name': 'Bob', 'phone': '2222'})

		# Remove all
		self.db.processRequest(DeleteRequest('2'))
		self.db.processRequest(DeleteRequest('3'))

		# Add object again
		self.addObject('2', {'name': 'Alex', 'phone': '2222'})

		# Check that addition was successful
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '2222')))

		self.checkSearchResult(res, ['2'])

	def testSearchNonExistentFieldInAndCondition(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})
		self.addObject('3', {'name': 'Carl', 'phone': '3333', 'age': '27'})
		self.addObject('4', {'name': 'Don', 'phone': '4444', 'age': '20'})
		self.addObject('5', {'name': 'Alex', 'phone': '1111', 'age': '22'})

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '1111'
			),
		SearchRequest.And(),
		SearchRequest.Condition(
			Field('blablabla'), SearchRequest.Eq(), '22', invert=True
			)
		)))

		self.checkSearchResult(res, [])

	def testSearchNonExistentFieldInOrCondition(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})
		self.addObject('3', {'name': 'Carl', 'phone': '3333', 'age': '27'})
		self.addObject('4', {'name': 'Don', 'phone': '4444', 'age': '20'})
		self.addObject('5', {'name': 'Alex', 'phone': '1111', 'age': '22'})

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '1111'
			),
		SearchRequest.Or(),
		SearchRequest.Condition(
			Field('blablabla'), SearchRequest.Eq(), '22', invert=True
			)
		)))

		self.checkSearchResult(res, ['1', '5'])

	def testReadAllFields(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})

		res = self.db.processRequest(ReadRequest('1'))
		self.checkReadResult(res, [
			Field('name', 'text', 'Alex'),
			Field('phone', 'text', '1111')])

	def testReadSomeFields(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})

		res = self.db.processRequest(ReadRequest('1', [Field('name')]))
		self.checkReadResult(res, [
			Field('name', 'text', 'Alex'),
			])

	def testReadNonExistingField(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})

		res = self.db.processRequest(ReadRequest('1', [Field('name'), Field('age')]))
		self.checkReadResult(res, [
			Field('name', 'text', 'Alex')
			])

def suite():
	res = testhelpers.NamedTestSuite()

	parameters = [
		('memory.sqlite3', database.Sqlite3Database, ':memory:'),
	]

	for parameter in parameters:
		res.addTest(unittest.TestLoader().loadTestsFromTestCase(
			TestRequests.getParameterized(*parameter)))

	return res

if __name__ == '__main__':
	testhelpers.TextTestRunner(verbosity=2).run(suite())
