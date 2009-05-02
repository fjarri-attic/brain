import unittest

import testhelpers
import database
from interfaces import *

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
		
		to_compare = zip(res, expected)
		for res_elem, expected_elem in to_compare:
			self.failUnless(res_elem == expected_elem, 
				"Search result is wrong: " + str(res_elem) + 
				", expected: " + str(expected_elem))
	
	def addObject(self, id, fields={}):
		field_objs = [Field(key if isinstance(key, list) else [key], 
			'text', fields[key]) for key in fields.keys()]
		self.db.processRequest(ModifyRequest(id, field_objs))
		
	def testBlankObjectAddition(self):
		self.addObject('1')

	def testAdditionNoCheck(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		
	def testAddition(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			['phone'], SearchRequest.Eq, '1111')))
		
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
			['name'], SearchRequest.Eq, 'Bob')))

		self.checkSearchResult(res, ['2'])	
			
	def testModificationPreservesFields(self):
		self.addObject('2', {'name': 'Alex', 'phone': '1111'})

		self.db.processRequest(ModifyRequest('2', [
			Field('name', 'text', 'Bob')
		]))
		
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			['phone'], SearchRequest.Eq, '1111')))

		self.checkSearchResult(res, ['2'])	
			
	def testMultipleModification(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})

		self.db.processRequest(ModifyRequest('2', [
			Field(['name'], 'text', 'Rob')
		]))
		
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			['name'], SearchRequest.Eq, 'Rob')))

		self.checkSearchResult(res, ['2'])
	
	def testSearchConditionAnd(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})
		self.addObject('3', {'name': 'Carl', 'phone': '3333', 'age': '27'})
		self.addObject('4', {'name': 'Don', 'phone': '4444', 'age': '20'})
		self.addObject('5', {'name': 'Alex', 'phone': '1111', 'age': '22'})
		
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			['phone'], SearchRequest.Eq, '1111'
			),
		SearchRequest.And,
		SearchRequest.Condition(
			['age'], SearchRequest.Eq, '22'
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
			['phone'], SearchRequest.Eq, '2222'
			),
		SearchRequest.Or,
		SearchRequest.Condition(
			['age'], SearchRequest.Eq, '27'
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
			['phone'], SearchRequest.Eq, '1111'
			),
		SearchRequest.And,
		SearchRequest.Condition(
			['age'], SearchRequest.Eq, '22', invert=True
			)
		)))
		
		self.checkSearchResult(res, ['1'])
	
	def testSearchConditionRegexp(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})
		self.addObject('3', {'name': 'Carl', 'phone': '3333', 'age': '27'})
		self.addObject('4', {'name': 'Don', 'phone': '4444', 'age': '20'})
		self.addObject('5', {'name': 'Alex', 'phone': '1111', 'age': '22'})
		
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			['phone'], SearchRequest.Regexp, '\d+'
			),
		SearchRequest.And,
		SearchRequest.Condition(
			['age'], SearchRequest.Eq, '22', invert=True
			)
		)))
		
		self.checkSearchResult(res, ['1', '2', '3', '4'])
	
	def testDeleteObject(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})
		self.addObject('3', {'name': 'Carl', 'phone': '3333', 'age': '27'})

		self.db.processRequest(DeleteRequest('3'))
		
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			['phone'], SearchRequest.Eq, '3333')))
		self.checkSearchResult(res, [])

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			['phone'], SearchRequest.Eq, '1111')))
		self.checkSearchResult(res, ['1'])

	def testDeleteExistentFields(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})
		self.addObject('3', {'name': 'Carl', 'phone': '3333', 'age': '27'})

		self.db.processRequest(DeleteRequest('3', [Field('age'), Field('phone')]))
		
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			['name'], SearchRequest.Eq, 'Carl')))
		self.checkSearchResult(res, ['3'])

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			['phone'], SearchRequest.Eq, '3333')))
		self.checkSearchResult(res, [])

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			['age'], SearchRequest.Eq, '27')))
		self.checkSearchResult(res, [])
		
	def testDeleteNonExistentFields(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})
		self.addObject('3', {'name': 'Carl', 'phone': '3333', 'age': '27'})

		self.db.processRequest(DeleteRequest('2', [Field('name'), Field('blablabla')]))
		
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			['name'], SearchRequest.Eq, 'Bob')))
		self.checkSearchResult(res, [])

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			['phone'], SearchRequest.Eq, '2222')))
		self.checkSearchResult(res, ['2'])
	
	def testSearchNonExistentFieldInAndCondition(self):
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})
		self.addObject('3', {'name': 'Carl', 'phone': '3333', 'age': '27'})
		self.addObject('4', {'name': 'Don', 'phone': '4444', 'age': '20'})
		self.addObject('5', {'name': 'Alex', 'phone': '1111', 'age': '22'})
		
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			['phone'], SearchRequest.Eq, '1111'
			),
		SearchRequest.And,
		SearchRequest.Condition(
			['blablabla'], SearchRequest.Eq, '22', invert=True
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
			['phone'], SearchRequest.Eq, '1111'
			),
		SearchRequest.Or,
		SearchRequest.Condition(
			['blablabla'], SearchRequest.Eq, '22', invert=True
			)
		)))
		
		self.checkSearchResult(res, ['1', '5'])


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
