"""Unit-tests for database layer interface"""

import unittest

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import brain
import brain.op as op
import helpers
from brain.interface import *
from brain.database import Field


class Format(unittest.TestCase):
	"""Class which contains all request format testcases"""

	# Tests for Field class

	def testFieldInitWithStr(self):
		"""Test field creation with string name"""
		f = Field(None, ['test'], '1')
		self.assertEqual(f.name, ['test'])

	def testFieldInitWithList(self):
		"""Test field creation with list name"""
		f = Field(None, ['test', 1, None], '1')

	def testFieldInitWithMap(self):
		"""Test field creation with map name"""
		self.assertRaises(brain.FormatError, Field, None, {'test': 1}, '1')

	def testFieldInitWithWrongList(self):
		"""Test field creation with wrong element in name list"""
		self.assertRaises(brain.FormatError, Field, None, ['test', 1, [1, 2]], '1')

	def testFieldInitWithEmptyName(self):
		"""Test field creation with empty string as a name"""
		self.assertRaises(brain.FormatError, Field, None, '', '1')

	def testFieldInitWithNoName(self):
		"""Test field creation with None as a name"""
		self.assertRaises(brain.FormatError, Field, None, None, '1')

	def testFieldCopiesList(self):
		"""Regression for bug when Field did not copy initializing list"""
		l = ['test', 1]
		f = Field(None, l, '1')
		l[1] = 2
		self.assertEqual(f.name[1], 1)

	def testFieldEq(self):
		"""Test == operator for equal fields"""
		f1 = Field(None, ['test', 1, None], 1)
		f2 = Field(None, ['test', 1, None], 1)
		self.assertEqual(f1, f2)

	def testFieldNonEqNames(self):
		"""Test == operator for fields with different names"""
		f1 = Field(None, ['test1', 1, None], 1)
		f2 = Field(None, ['test2', 2, None], 1)
		self.failIfEqual(f1, f2)

	def testFieldNonEqValues(self):
		"""Test == operator for fields with different values"""
		f1 = Field(None, ['test1', 1, None], 1)
		f2 = Field(None, ['test2', 1, None], 2)
		self.failIfEqual(f1, f2)

	def testFieldEmptyName(self):
		"""Check that field cannot have empty name"""
		self.assertRaises(brain.FormatError, Field, None, None, 1)
		self.assertRaises(brain.FormatError, Field, None, "", 1)

	def testModifyRequestNoFields(self):
		"""Test that request can be created without fields"""
		r = ModifyRequest('1')

	def testCreateRequestEmptyData(self):
		"""Test that creation request will fail without data provided"""
		self.assertRaises(FormatError, CreateRequest, None)

	# Additional checks for InsertRequest

	def testInsertRequestNotDeterminedTarget(self):
		"""Test that InsertRequest requires determined target"""
		self.assertRaises(brain.FormatError, InsertRequest,
			'1', Field(None, ['test', None, 1]),
			[Field(None, ['test'], 1)])

	def testInsertRequestTargetPointsToMap(self):
		"""Test that InsertRequest requires target pointing to list"""
		self.assertRaises(brain.FormatError, InsertRequest,
			'1', Field(None, ['test', 1, 'aaa']),
			[Field(None, ['test'], 1)])

	def testInsertRequestNotDeterminedField(self):
		"""Test that InsertRequest requires determined fields to insert"""
		self.assertRaises(brain.FormatError, InsertRequest,
			'1', Field(None, ['test', 1, 'aaa']),
			[Field(None, ['test'], None)])

	# Checks for SearchRequest

	def testSearchRequestProperlyFormed(self):
		"""Test that properly formed SearchRequest does not raise anything"""
		SearchRequest(SearchRequest.Condition(
			SearchRequest.Condition(
				Field(None, ['phone']), op.EQ, '1111'
			),
			op.OR,
			SearchRequest.Condition(
				SearchRequest.Condition(
					Field(None, ['phone']), op.EQ, '1111'
				),
				op.AND,
				SearchRequest.Condition(
					Field(None, ['blablabla']), op.REGEXP, '22', invert=True
				)
			)
		))

	def testSearchRequestFirstOperandIsNotCondition(self):
		"""Test that condition raises error if first operand in node is not Condition"""
		self.assertRaises(brain.FormatError, SearchRequest.Condition,
			Field(None, ['aaa'], 1),
			op.AND,
			SearchRequest.Condition(
				Field(None, ['blablabla']), op.REGEXP, '22', invert=True
			)
		)

	def testSearchRequestSecondOperandIsNotCondition(self):
		"""Test that condition raises error if second operand in node is not Condition"""
		self.assertRaises(brain.FormatError, SearchRequest.Condition,
			SearchRequest.Condition(
				Field(None, ['blablabla']), op.REGEXP, '22', invert=True
			),
			op.AND,
			Field(None, ['aaa'], 1)
		)

	def testSearchRequestWrongConditionOperator(self):
		"""Test that condition raises error if condition operator is unknown"""
		self.assertRaises(brain.FormatError, SearchRequest.Condition,
			SearchRequest.Condition(
				Field(None, ['phone']), op.EQ, '1111'
			),
			'something',
			SearchRequest.Condition(
				Field(None, ['blablabla']), op.EQ, '22', invert=True
			)
		)

	def testSearchRequestWrongComparisonOperator(self):
		"""Test that condition raises error if comparison operator is unknown"""
		self.assertRaises(brain.FormatError, SearchRequest.Condition,
			'phone', 'something', '1111'
		)

	def testSearchRequestConditionEqSupportedTypes(self):
		"""Test that all necessary types are supported for equality check"""
		classes = [str, int, float, bytes]

		for cls in classes:
			c = SearchRequest.Condition(Field(None, ['fld']), op.EQ, cls())

	def testSearchRequestConditionRegexpSupportedTypes(self):
		"""Test that only strings and bytearrays are supported for regexps"""

		def construct_condition(cls):
			return SearchRequest.Condition(Field(None, ['fld']), op.REGEXP, cls())

		classes = [str, int, float, bytes]
		supported_classes = [str, bytes]

		for cls in classes:
			if cls in supported_classes:
				construct_condition(cls)
			else:
				self.assertRaises(brain.FormatError, construct_condition, cls)

	def testNoneInSearchCondition(self):
		"""Test that None value can be used in search condition"""

		# check that Nones can be used in equalities
		SearchRequest.Condition(Field(None, ['fld']), op.EQ, None)

		# check that Nones cannot be used with other operators
		for operator in [op.REGEXP, op.LT, op.LTE, op.GT, op.GTE]:
			self.assertRaises(brain.FormatError, SearchRequest.Condition,
				Field(None, ['fld']), operator, None)

def suite():
	"""Generate test suite for this module"""
	res = helpers.NamedTestSuite()
	res.addTest(unittest.TestLoader().loadTestsFromTestCase(Format))
	return res
