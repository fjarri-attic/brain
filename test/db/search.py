"""Unit tests for database layer search request"""

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import unittest
from test import helpers
from test.db.requests import TestRequest
from db.database import *
from db.interface import *

class Search(TestRequest):
	"""Test operation of SearchRequest"""

	def testConditionAnd(self):
		"""Check complex condition with And operator"""
		self.prepareStandNoList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.EQ, '1111'
			),
		SearchRequest.AND,
		SearchRequest.Condition(
			Field('age'), SearchRequest.EQ, '22'
			)
		)))

		self.checkRequestResult(res, [self.id5])

	def testConditionOr(self):
		"""Check complex condition with Or operator"""
		self.prepareStandNoList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.EQ, '2222'
			),
		SearchRequest.OR,
		SearchRequest.Condition(
			Field('age'), SearchRequest.EQ, '27'
			)
		)))

		self.checkRequestResult(res, [self.id2, self.id3])

	def testConditionInvert(self):
		"""Check operation of inversion flag in condition"""
		self.prepareStandNoList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.EQ, '1111'
			),
		SearchRequest.AND,
		SearchRequest.Condition(
			Field('age'), SearchRequest.EQ, '22', invert=True
			)
		)))

		self.checkRequestResult(res, [self.id1])

	def testConditionInvertInRootAnd(self):
		"""Check if inversion flag works in the root of condition with AND"""
		self.prepareStandNoList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.EQ, '1111'
			),
		SearchRequest.AND,
		SearchRequest.Condition(
			Field('age'), SearchRequest.EQ, '22', invert=True
			),
		invert=True
		)))

		self.checkRequestResult(res, [self.id2, self.id3, self.id4, self.id5])

	def testConditionInvertInRootOr(self):
		"""Check if inversion flag works in the root of condition with OR"""
		self.prepareStandNoList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.EQ, '1111'
			),
		SearchRequest.OR,
		SearchRequest.Condition(
			Field('age'), SearchRequest.EQ, '27', invert=True
			),
		invert=True
		)))

		self.checkRequestResult(res, [self.id3])

	def testConditionRegexp(self):
		"""Check operation of regexp-based condition"""
		self.prepareStandNoList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.REGEXP, '\d+'
			),
		SearchRequest.AND,
		SearchRequest.Condition(
			Field('age'), SearchRequest.EQ, '22', invert=True
			)
		)))

		self.checkRequestResult(res, [self.id1, self.id2, self.id3, self.id4])

	def testConditionRegexpOnPart(self):
		"""Regression for bug when there was match() instead of search() in regexp callback"""
		self.prepareStandNoList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('name'), SearchRequest.REGEXP, 'ex'
			)))

		self.checkRequestResult(res, [self.id1, self.id5])

	def testNonExistentFieldInAndCondition(self):
		"""Check that condition on non-existent field works with And operator"""
		self.prepareStandNoList()

		# Second part of the condition should return empty list,
		# so the whole result should be empty too
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field('phone'), SearchRequest.EQ, '1111'
			),
		SearchRequest.AND,
		SearchRequest.Condition(
			Field('blablabla'), SearchRequest.EQ, '22', invert=True
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
			Field('phone'), SearchRequest.EQ, '1111'
			),
		SearchRequest.OR,
		SearchRequest.Condition(
			Field('blablabla'), SearchRequest.EQ, '22', invert=True
			)
		)))

		self.checkRequestResult(res, [self.id1, self.id5])

	def testListOneLevel(self):
		"""Check searching in simple lists"""
		self.prepareStandSimpleList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', None]), SearchRequest.EQ, 'Track 2'
			)))

		self.checkRequestResult(res, [self.id1, self.id2])

	def testListOneLevelDefinedPosition(self):
		"""Check search when some of positions in lists are defined"""
		self.prepareStandSimpleList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', 1]), SearchRequest.EQ, 'Track 2'
			)))

		self.checkRequestResult(res, [self.id1])

	def testNestedList(self):
		"""Check searching in nested lists"""
		self.prepareStandNestedList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', 0, 'Authors', None]), SearchRequest.EQ, 'Alex'
			)))

		self.checkRequestResult(res, [self.id1])

	def testNestedListRegexp(self):
		"""Check regexp searching in nested lists"""
		self.prepareStandNestedList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', 1, 'Authors', None]), SearchRequest.REGEXP, 'Carl'
			)))

		self.checkRequestResult(res, [self.id1])

	def testNestedListComplexCondition(self):
		"""Check search in nested list with complex condition"""
		self.prepareStandNestedList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field(['tracks', None, 'Authors', 1]), SearchRequest.EQ, 'Bob'
			),
		SearchRequest.AND,
		SearchRequest.Condition(
			Field(['tracks', None, 'Name']), SearchRequest.REGEXP, 'name'
			)
		)))

		self.checkRequestResult(res, [self.id1])

	def testConditionGreater(self):
		"""Test that '>' search condition"""
		self.prepareStandDifferentTypes()

		# check work for ints

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', None, 'Length']), SearchRequest.GT, 310
			)))

		self.checkRequestResult(res, [self.id2])

		# check work for strings

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', None, 'Name']), SearchRequest.GT, 'Track 2 name'
			)))

		self.checkRequestResult(res, [self.id2])

	def testConditionGreaterOrEqual(self):
		"""Test that '>=' search condition"""
		self.prepareStandDifferentTypes()

		# check work for ints

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', None, 'Length']), SearchRequest.GTE, 300
			)))

		self.checkRequestResult(res, [self.id1, self.id2])

		# check work for strings

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', None, 'Name']), SearchRequest.GTE, 'Track 2 name'
			)))

		self.checkRequestResult(res, [self.id1, self.id2])

	def testConditionLower(self):
		"""Test that '<' search condition"""
		self.prepareStandDifferentTypes()

		# check work for ints

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', None, 'Length']), SearchRequest.LT, 300
			)))

		self.checkRequestResult(res, [self.id2])

		# check work for strings

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', None, 'Name']), SearchRequest.LT, 'Track 3 name'
			)))

		self.checkRequestResult(res, [self.id1])

	def testConditionLowerOrEqual(self):
		"""Test that '<=' search condition"""
		self.prepareStandDifferentTypes()

		# check work for ints

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', None, 'Volume']), SearchRequest.LTE, 27.0
			)))

		self.checkRequestResult(res, [self.id2])

		# check work for strings

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', None, 'Name']), SearchRequest.LTE, 'Track 3 name'
			)))

		self.checkRequestResult(res, [self.id1, self.id2])

	def testNoneTypeSimpleCondition(self):
		"""Test that search condition works for NULL value"""
		self.prepareStandDifferentTypes()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', None, 'Volume']), SearchRequest.EQ, None
			)))

		self.checkRequestResult(res, [self.id2])

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', None, 'Volume']), SearchRequest.EQ, None,
			invert=True
			)))

		self.checkRequestResult(res, [self.id1, self.id3])

	def testNoneTypeComplexCondition(self):
		"""Test that complex search condition works for NULL value"""
		self.prepareStandDifferentTypes()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
		SearchRequest.Condition(
			Field(['tracks', None, 'Volume']), SearchRequest.EQ, None, invert=True
			),
		SearchRequest.AND,
		SearchRequest.Condition(
			Field(['tracks', None, 'Length']), SearchRequest.EQ, None
			)
		)))

		self.checkRequestResult(res, [self.id3])

def get_class():
	return Search
