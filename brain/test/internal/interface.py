"""Functionality tests for constructors of internal request classes"""

import unittest

import brain
import brain.op as op
from brain.interface import *

import helpers

class Format(helpers.NamedTestCase):
	"""Class which contains all request format testcases"""

	# Tests for Field class

	def testFieldWrongName(self):
		"""Test that Field constructor raises exceptions on wrong names"""
		names = ["fld", {'test': 1}, ['test', 1, [1, 2]], [''], None, '', ["UpperCaseName"]]

		for name in names:
			self.assertRaises(brain.FormatError, Field, None, name, '1')

	def testFieldWrongValue(self):
		"""Test that Field constructor raises exceptions on wrong values"""
		values = [set([1, 2]), bytearray(b'aaaa')]

		for value in values:
			self.assertRaises(brain.FormatError, Field, None, ['fld'], value)

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
		self.assertNotEqual(f1, None)
		self.assertNotEqual(f1, 1)

	def testFieldNonEqNames(self):
		"""Test == operator for fields with different names"""
		f1 = Field(None, ['test1', 1, None], 1)
		f2 = Field(None, ['test2', 2, None], 1)
		self.assertNotEqual(f1, f2)

	def testFieldNonEqValues(self):
		"""Test == operator for fields with different values"""
		f1 = Field(None, ['test1', 1, None], 1)
		f2 = Field(None, ['test2', 1, None], 2)
		self.assertNotEqual(f1, f2)

	# Tests for request constructors

	def testCreateRequestEmptyData(self):
		"""Test that creation request will fail without data provided"""
		self.assertRaises(FormatError, CreateRequest, None)
		self.assertRaises(FormatError, CreateRequest, [])

	def testRequestWithNoID(self):
		"""Test that requests throw exceptions when no ID is provided"""
		field = Field(None, ['fld', 1], 1)
		fields = [field]
		self.assertRaises(brain.FormatError, ModifyRequest, None, field, fields, True)
		self.assertRaises(brain.FormatError, ReadRequest, None, field)
		self.assertRaises(brain.FormatError, DeleteRequest, None, field)
		self.assertRaises(brain.FormatError, InsertRequest, None, field, [fields], True)

	# Additional checks for ReadRequest

	def testReadRequestNotDeterminedPath(self):
		"""Test that ReadRequest requires determined path"""
		self.assertRaises(brain.FormatError, ReadRequest,
			'1', Field(None, ['test', None, 1]),
			[[Field(None, ['test'], 1)]])

	# Additional checks for InsertRequest

	def testInsertRequestNotDeterminedTarget(self):
		"""Test that InsertRequest requires determined target"""
		self.assertRaises(brain.FormatError, InsertRequest,
			'1', Field(None, ['test', None, 1]),
			[[Field(None, ['test'], 1)]], True)

	def testInsertRequestTargetPointsToMap(self):
		"""Test that InsertRequest requires target pointing to list"""
		self.assertRaises(brain.FormatError, InsertRequest,
			'1', Field(None, ['test', 1, 'aaa']),
			[[Field(None, ['test'], 1)]], True)

	def testInsertRequestNotDeterminedField(self):
		"""Test that InsertRequest requires determined fields to insert"""
		self.assertRaises(brain.FormatError, InsertRequest,
			'1', Field(None, ['test', 1, 2]),
			[[Field(None, ['test', None])]], True)

	# Additional checks for SearchRequest

	def testSearchRequestFirstOperandIsNotCondition(self):
		"""Test that condition raises error if first operand in node is not Condition"""
		self.assertRaises(brain.FormatError, SearchRequest.Condition,
			Field(None, ['aaa'], 1),
			op.AND,
			SearchRequest.Condition(
				Field(None, ['blablabla']), op.REGEXP,
				Field(None, [], '22'), invert=True
			)
		)

	def testSearchRequestSecondOperandIsNotCondition(self):
		"""Test that condition raises error if second operand in node is not Condition"""
		self.assertRaises(brain.FormatError, SearchRequest.Condition,
			SearchRequest.Condition(
				Field(None, ['blablabla']), op.REGEXP,
				Field(None, [], '22'), invert=True
			),
			op.AND,
			Field(None, ['aaa'], 1)
		)

	def testSearchRequestWrongConditionOperator(self):
		"""Test that condition raises error if condition operator is unknown"""
		self.assertRaises(brain.FormatError, SearchRequest.Condition,
			SearchRequest.Condition(
				Field(None, ['phone']), op.EQ, Field(None, [], '1111')
			),
			'something',
			SearchRequest.Condition(
				Field(None, ['blablabla']), op.EQ, Field(None, [], '22'), invert=True
			)
		)

	def testSearchRequestWrongComparisonOperator(self):
		"""Test that condition raises error if comparison operator is unknown"""
		self.assertRaises(brain.FormatError, SearchRequest.Condition,
			'phone', 'something', '1111'
		)

	def testSearchRequestConditionEqSupportedTypes(self):
		"""Test that all necessary types are supported for equality check"""
		classes = [str, int, float, bytes, list, dict]

		for cls in classes:
			c = SearchRequest.Condition(Field(None, ['fld']), op.EQ,
				Field(None, [], cls()))

	def testSearchRequestConditionRegexpSupportedTypes(self):
		"""Test that only strings and bytearrays are supported for regexps"""

		def construct_condition(cls):
			return SearchRequest.Condition(Field(None, ['fld']), op.REGEXP,
				Field(None, [], cls()))

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
		SearchRequest.Condition(Field(None, ['fld']), op.EQ, Field(None, [], None))

		# check that Nones cannot be used with other operators
		for operator in [op.REGEXP, op.LT, op.LTE, op.GT, op.GTE]:
			self.assertRaises(brain.FormatError, SearchRequest.Condition,
				Field(None, ['fld']), operator, Field(None, [], None))

	def testObjectExistsCreation(self):
		"""Simple test for ObjectExistsRequest constructor"""
		self.assertRaises(brain.FormatError, ObjectExistsRequest, None)

	def testPointerConstructor(self):
		"""Check that required set of types can be passed to pointer"""
		for val in [None, {}, []]:
			p = Pointer.fromPyValue(val)

		for val in [set(), bytearray(b'a')]:
			self.assertRaises(FormatError, Pointer.fromPyValue, val)

	def testStrAndRepr(self):
		"""
		Tests, intended to make coverage counter happy
		str() and repr() are not used during normal operations, but I want
		to keep them for debugging purposes
		"""
		temp_id = 1
		f = Field(None, ['aaa', 1], '1')

		objs = [
			    CreateRequest([f]),
			    ModifyRequest(temp_id, f, [f], True),
			    ReadRequest(temp_id, f, [f]),
			    InsertRequest(temp_id, f, [[f]], True),
			    DeleteRequest(temp_id, [[f]]),
			    SearchRequest(SearchRequest.Condition(f, op.EQ, f)),
			    Pointer.fromPyValue(None),
			    Pointer.fromPyValue([]),
			    Pointer.fromPyValue({}),
			    f,
			    ObjectExistsRequest(temp_id),
			    DumpRequest(),
			    RepairRequest()
		]

		for obj in objs:
			s = str(obj)
			r = repr(obj)


def suite():
	"""Generate test suite for this module"""
	res = helpers.NamedTestSuite('interface')
	res.addTestCaseClass(Format)
	return res
