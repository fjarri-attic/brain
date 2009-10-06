"""Unit tests for database layer search request"""

import unittest

import brain
import brain.op as op

import helpers
from public.requests import TestRequest, getParameterized

class Search(TestRequest):
	"""Test operation of SearchRequest"""

	def testConditionAnd(self):
		"""Check complex condition with And operator"""
		self.prepareStandNoList()
		res = self.conn.search(
			[['phone'], op.EQ, '1111'],
			op.AND,
			[['age'], op.EQ, '22']
		)

		self.assertEqual(res, [self.id5])

	def testConditionOr(self):
		"""Check complex condition with Or operator"""
		self.prepareStandNoList()
		res = self.conn.search(
			[['phone'], op.EQ, '2222'],
			op.OR,
			[['age'], op.EQ, '27']
		)

		self.assertSameElements(res, [self.id2, self.id3])

	def testConditionInvert(self):
		"""Check operation of inversion flag in condition"""
		self.prepareStandNoList()

		res = self.conn.search(
			[['phone'], op.EQ, '1111'],
			op.AND,
			[op.NOT, ['age'], op.EQ, '22']
		)

		self.assertEqual(res, [self.id1])

	def testConditionInvertInRootAnd(self):
		"""Check if inversion flag works in the root of condition with AND"""
		self.prepareStandNoList()

		res = self.conn.search(
			op.NOT,
			[[['phone'], op.EQ, '1111'],
			op.AND,
			[op.NOT, ['age'], op.EQ, '22']],
		)

		self.assertSameElements(res, [self.id2, self.id3, self.id4, self.id5])

	def testConditionInvertInRootOr(self):
		"""Check if inversion flag works in the root of condition with OR"""
		self.prepareStandNoList()

		res = self.conn.search(
			op.NOT,
			[[['phone'], op.EQ, '1111'],
			op.OR,
			[op.NOT, ['age'], op.EQ, '27']]
		)

		self.assertEqual(res, [self.id3])

	def testConditionRegexp(self):
		"""Check operation of regexp-based condition"""
		self.prepareStandNoList()

		res = self.conn.search(
			[['phone'], op.REGEXP, '\d+'],
			op.AND,
			[op.NOT, ['age'], op.EQ, '22']
		)

		self.assertSameElements(res, [self.id1, self.id2, self.id3, self.id4])

	def testConditionRegexpOnPart(self):
		"""Regression for bug when there was match() instead of search() in regexp callback"""
		self.prepareStandNoList()
		res = self.conn.search(['name'], op.REGEXP, 'ex')
		self.assertSameElements(res, [self.id1, self.id5])

	def testNonExistentFieldInAndCondition(self):
		"""Check that condition on non-existent field works with And operator"""
		self.prepareStandNoList()

		# Second part of the condition should return list of all objects
		res = self.conn.search(
			[['phone'], op.EQ, '1111'],
			op.AND,
			[op.NOT, ['blablabla'], op.EQ, '22']
		)

		self.assertSameElements(res, [self.id1, self.id5])

		# Same test, but without NOT
		res = self.conn.search(
			[['phone'], op.EQ, '1111'],
			op.AND,
			[['blablabla'], op.EQ, '22'] # empty set
		)

		self.assertEqual(res, [])

		# Same test, but now non-existent field goes first
		res = self.conn.search(
			[op.NOT, ['blablabla'], op.EQ, '22'],
			op.AND,
			[['phone'], op.EQ, '1111']
		)

		self.assertSameElements(res, [self.id1, self.id5])

		# Same test, but without NOT
		res = self.conn.search(
			[['blablabla'], op.EQ, '22'], # empty set
			op.AND,
			[['phone'], op.EQ, '1111']
		)

		self.assertEqual(res, [])

	def testNonExistentFieldInOrCondition(self):
		"""Check that condition on non-existent field works with Or operator"""
		self.prepareStandNoList()

		all = [self.id1, self.id2, self.id3, self.id4, self.id5]

		# Second part of the condition should return list of all objects
		res = self.conn.search(
			[['phone'], op.EQ, '1111'],
			op.OR,
			[op.NOT, ['blablabla'], op.EQ, '22']
		)

		self.assertSameElements(res, all)

		# Same test, but without NOT
		res = self.conn.search(
			[['phone'], op.EQ, '1111'],
			op.OR,
			[['blablabla'], op.EQ, '22'] # empty set
		)

		self.assertSameElements(res, [self.id1, self.id5])

		# Same test, but now non-existent field goes first
		res = self.conn.search(
			[op.NOT, ['blablabla'], op.EQ, '22'],
			op.OR,
			[['phone'], op.EQ, '1111']
		)

		self.assertSameElements(res, all)

		# Same test, but without NOT
		res = self.conn.search(
			[['blablabla'], op.EQ, '22'], # empty set
			op.OR,
			[['phone'], op.EQ, '1111']
		)

		self.assertSameElements(res, [self.id1, self.id5])

	def testNonExistentFieldInBothParts(self):
		"""
		Check that request work if both parts of complex condition mention
		non-existent field
		"""
		self.prepareStandNoList()
		res = self.conn.search(
			[['foobar'], op.EQ, '1111'], # empty set
			op.OR,
			[['blablabla'], op.EQ, '22'] # empty set
		)

		self.assertEqual(res, [])

		res = self.conn.search(
			[['foobar'], op.EQ, '1111'], # empty set
			op.OR,
			[op.NOT, ['blablabla'], op.EQ, '22'] # all objects
		)

		self.assertSameElements(res, [self.id1, self.id2, self.id3, self.id4, self.id5])

		res = self.conn.search(
			[op.NOT, ['foobar'], op.EQ, '1111'], # all objects
			op.OR,
			[['blablabla'], op.EQ, '22'] # empty set
		)

		self.assertSameElements(res, [self.id1, self.id2, self.id3, self.id4, self.id5])

	def testListOneLevel(self):
		"""Check searching in simple lists"""
		self.prepareStandSimpleList()
		res = self.conn.search(['tracks', None], op.EQ, 'Track 2')
		self.assertSameElements(res, [self.id1, self.id2])

	def testListOneLevelDefinedPosition(self):
		"""Check search when some of positions in lists are defined"""
		self.prepareStandSimpleList()
		res = self.conn.search(['tracks', 1], op.EQ, 'Track 2')
		self.assertEqual(res, [self.id1])

	def testNestedList(self):
		"""Check searching in nested lists"""
		self.prepareStandNestedList()
		res = self.conn.search(['tracks', 0, 'Authors', None], op.EQ, 'Alex')
		self.assertEqual(res, [self.id1])

	def testNestedListRegexp(self):
		"""Check regexp searching in nested lists"""
		self.prepareStandNestedList()
		res = self.conn.search(['tracks', 1, 'Authors', None], op.REGEXP, 'Carl')
		self.assertEqual(res, [self.id1])

	def testNestedListComplexCondition(self):
		"""Check search in nested list with complex condition"""
		self.prepareStandNestedList()

		res = self.conn.search(
			[['tracks', None, 'Authors', 1], op.EQ, 'Bob'],
			op.AND,
			[['tracks', None, 'Name'], op.REGEXP, 'name']
		)

		self.assertEqual(res, [self.id1])

	def testConditionGreater(self):
		"""Test that '>' search condition"""
		self.prepareStandDifferentTypes()

		# check work for ints
		res = self.conn.search(['tracks', None, 'Length'], op.GT, 310)
		self.assertEqual(res, [self.id2])

		# check work for strings
		res = self.conn.search(['tracks', None, 'Name'], op.GT, 'Track 2 name')
		self.assertEqual(res, [self.id2])

	def testConditionGreaterOrEqual(self):
		"""Test that '>=' search condition"""
		self.prepareStandDifferentTypes()

		# check work for ints
		res = self.conn.search(['tracks', None, 'Length'], op.GTE, 300)
		self.assertSameElements(res, [self.id1, self.id2])

		# check work for strings
		res = self.conn.search(['tracks', None, 'Name'], op.GTE, 'Track 2 name')
		self.assertSameElements(res, [self.id1, self.id2])

	def testConditionLower(self):
		"""Test that '<' search condition"""
		self.prepareStandDifferentTypes()

		# check work for ints
		res = self.conn.search(['tracks', None, 'Length'], op.LT, 300)
		self.assertEqual(res, [self.id2])

		# check work for strings
		res = self.conn.search(['tracks', None, 'Name'], op.LT, 'Track 3 name')
		self.assertEqual(res, [self.id1])

	def testConditionLowerOrEqual(self):
		"""Test that '<=' search condition"""
		self.prepareStandDifferentTypes()

		# check work for ints
		res = self.conn.search(['tracks', None, 'Volume'], op.LTE, 27.0)
		self.assertEqual(res, [self.id2])

		# check work for strings
		res = self.conn.search(['tracks', None, 'Name'], op.LTE, 'Track 3 name')
		self.assertSameElements(res, [self.id1, self.id2])

	def testNoneTypeSimpleCondition(self):
		"""Test that search condition works for NULL value"""
		self.prepareStandDifferentTypes()

		res = self.conn.search(['tracks', None, 'Volume'], op.EQ, None)
		self.assertEqual(res, [self.id2])

		res = self.conn.search(op.NOT, ['tracks', None, 'Volume'], op.EQ, None)
		self.assertSameElements(res, [self.id1, self.id3])

	def testNoneTypeComplexCondition(self):
		"""Test that complex search condition works for NULL value"""
		self.prepareStandDifferentTypes()

		res = self.conn.search(
			[op.NOT, ['tracks', None, 'Volume'], op.EQ, None],
			op.AND,
			[['tracks', None, 'Length'], op.EQ, None]
		)

		self.assertEqual(res, [self.id3])

	def testWrongCondition(self):
		"""Test that exception is raised when condition is ill-formed"""
		self.assertRaises(brain.FormatError, self.conn.search,
			['tracks', None, 'Length'], op.EQ)

	def testGetAllIDs(self):
		"""Check that empty search condition returns list of all IDs in database"""
		self.prepareStandNoList()
		res = self.conn.search()
		self.assertSameElements(res, [self.id1, self.id2, self.id3, self.id4, self.id5])

	def testSearchForList(self):
		"""Check that one can search for list in hierarchy"""
		obj1 = self.conn.create({'aaa': 'val'})
		obj2 = self.conn.create({'aaa': [1, 2]})
		res = self.conn.search(['aaa'], op.EQ, list())
		self.assertEqual(res, [obj2])

	def testSearchForMap(self):
		"""Check that one can search for map in hierarchy"""
		obj1 = self.conn.create({'aaa': 'val'})
		obj2 = self.conn.create({'aaa': {'bbb': 'val'}})
		res = self.conn.search(['aaa'], op.EQ, dict())
		self.assertEqual(res, [obj2])

	def testLongCondition(self):
		"""Test work of long search conditions"""
		self.prepareStandNoList()

		# note that the result of this request depends on calculation order
		res = self.conn.search([op.NOT, ['name'], op.EQ, 'Alex'], op.AND,
			[op.NOT, ['phone'], op.EQ, '3333'], op.OR,
			[['age'], op.EQ, '22'])

		self.assertSameElements(res, [self.id2, self.id4, self.id5])

	def testLongConditionWithInversion(self):
		"""Test work of long search conditions"""
		self.prepareStandNoList()

		# inversion should be applied to the next operand only
		res = self.conn.search(op.NOT, [op.NOT, ['name'], op.EQ, 'Alex'], op.AND,
			[op.NOT, ['phone'], op.EQ, '3333'], op.OR,
			[['age'], op.EQ, '22'])

		self.assertSameElements(res, [self.id1, self.id5])

	def testTrivialCompoundCondition(self):
		"""Test that conditions like [NOT, condition] are supported"""
		self.prepareStandNoList()
		res = self.conn.search([op.NOT, [op.NOT, ['name'], op.EQ, 'Alex']],
			op.AND, [op.NOT, [['age'], op.EQ, '22']])

		self.assertEqual(res, [self.id1])

	def testWrongNumberOfElements(self):
		"""Test request validity checker - number of elements"""
		arg_sets = [
			(['age']),
			(op.NOT, ['age']),
			(['age'], '22'),
			(op.NOT, ['age'], '22'),
			(['age'], op.EQ, '22', 23, 24),
			(op.NOT, ['age'], op.EQ, '22', 23, 24),
			(op.NOT, [op.NOT, ['name'], op.EQ, 'Alex'], op.OR),
			(op.NOT, [op.NOT, ['name'], op.EQ, 'Alex'], op.OR, op.NOT)
		]

		for arg_set in arg_sets:
			self.assertRaises(brain.FormatError, self.conn.search, *arg_set)

	def testWrongCompoundCondition(self):
		"""Test request validity checker - mixed compound condition"""
		self.assertRaises(brain.FormatError, self.conn.search,
			op.NOT, [op.NOT, ['name'], op.EQ, 'Alex'], op.OR, ['name'])


def suite(engine_params, connection_generator):
	res = helpers.NamedTestSuite('search')
	res.addTestCaseClass(getParameterized(Search, engine_params, connection_generator))
	return res
