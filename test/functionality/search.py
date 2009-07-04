"""Unit tests for database layer search request"""

import unittest

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import brain
import brain.op as op
from test.functionality.requests import TestRequest

class Search(TestRequest):
	"""Test operation of SearchRequest"""

	def testConditionAnd(self):
		"""Check complex condition with And operator"""
		self.prepareStandNoList()
		res = self.conn.search(
			(['phone'], op.EQ, '1111'),
			op.AND,
			(['age'], op.EQ, '22')
		)

		self.assertEqual(res, [self.id5])

	def testConditionOr(self):
		"""Check complex condition with Or operator"""
		self.prepareStandNoList()
		res = self.conn.search(
			(['phone'], op.EQ, '2222'),
			op.OR,
			(['age'], op.EQ, '27')
		)

		self.assertSameElements(res, [self.id2, self.id3])

	def testConditionInvert(self):
		"""Check operation of inversion flag in condition"""
		self.prepareStandNoList()

		res = self.conn.search(
			(['phone'], op.EQ, '1111'),
			op.AND,
			(op.NOT, ['age'], op.EQ, '22')
		)

		self.assertEqual(res, [self.id1])

	def testConditionInvertInRootAnd(self):
		"""Check if inversion flag works in the root of condition with AND"""
		self.prepareStandNoList()

		res = self.conn.search(
			op.NOT,
			(['phone'], op.EQ, '1111'),
			op.AND,
			(op.NOT, ['age'], op.EQ, '22'),
		)

		self.assertSameElements(res, [self.id2, self.id3, self.id4, self.id5])

	def testConditionInvertInRootOr(self):
		"""Check if inversion flag works in the root of condition with OR"""
		self.prepareStandNoList()

		res = self.conn.search(
			op.NOT,
			(['phone'], op.EQ, '1111'),
			op.OR,
			(op.NOT, ['age'], op.EQ, '27')
		)

		self.assertEqual(res, [self.id3])

	def testConditionRegexp(self):
		"""Check operation of regexp-based condition"""
		self.prepareStandNoList()

		res = self.conn.search(
			(['phone'], op.REGEXP, '\d+'),
			op.AND,
			(op.NOT, ['age'], op.EQ, '22')
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

		# Second part of the condition should return empty list,
		# so the whole result should be empty too
		res = self.conn.search(
			(['phone'], op.EQ, '1111'),
			op.AND,
			(op.NOT, ['blablabla'], op.EQ, '22')
		)

		self.assertEqual(res, [])

	def testNonExistentFieldInOrCondition(self):
		"""Check that condition on non-existent field works with Or operator"""
		self.prepareStandNoList()

		# Second part of the condition should return empty list,
		# so the whole result should be equal to the result of the first part
		res = self.conn.search(
			(['phone'], op.EQ, '1111'),
			op.OR,
			(op.NOT, ['blablabla'], op.EQ, '22')
		)

		self.assertSameElements(res, [self.id1, self.id5])

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
			(['tracks', None, 'Authors', 1], op.EQ, 'Bob'),
			op.AND,
			(['tracks', None, 'Name'], op.REGEXP, 'name')
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
			(op.NOT, ['tracks', None, 'Volume'], op.EQ, None),
			op.AND,
			(['tracks', None, 'Length'], op.EQ, None)
		)

		self.assertEqual(res, [self.id3])

def get_class():
	return Search
