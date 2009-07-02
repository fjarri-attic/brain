"""Unit tests for database layer insert request"""

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import unittest
from test import helpers
from test.functionality.requests import TestRequest
from brain.database import *
from brain.interface import *

class Insert(TestRequest):
	"""Test operation of InsertRequest"""

	def testToTheMiddleSimpleList(self):
		"""Check insertion to the middle of simple list"""
		self.prepareStandSimpleList()

		res = self.db.processRequest(InsertRequest('1',
			Field(['tracks', 1]),
			[
				Field([], 'Track 4'),
				Field([], 'Track 5')
			]
		))

		res = self.db.processRequest(ReadRequest('1', [Field(['tracks', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0], 'Track 1'),
			Field(['tracks', 1], 'Track 4'),
			Field(['tracks', 2], 'Track 5'),
			Field(['tracks', 3], 'Track 2'),
			Field(['tracks', 4], 'Track 3'),
			])

	def testToTheBeginningSimpleList(self):
		"""Check insertion to the beginning of simple list"""
		self.prepareStandSimpleList()

		res = self.db.processRequest(InsertRequest('1',
			Field(['tracks', 0]),
			[
				Field([], 'Track 4'),
				Field([], 'Track 5')
			]
		))

		res = self.db.processRequest(ReadRequest('1', [Field(['tracks', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0], 'Track 4'),
			Field(['tracks', 1], 'Track 5'),
			Field(['tracks', 2], 'Track 1'),
			Field(['tracks', 3], 'Track 2'),
			Field(['tracks', 4], 'Track 3'),
			])

	def testToTheEndSimpleList(self):
		"""Check insertion to the end of simple list"""
		self.prepareStandSimpleList()

		res = self.db.processRequest(InsertRequest('1',
			Field(['tracks', None]),
			[
				Field([], 'Track 4'),
				Field([], 'Track 5')
			]
		))

		res = self.db.processRequest(ReadRequest('1', [Field(['tracks', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0], 'Track 1'),
			Field(['tracks', 1], 'Track 2'),
			Field(['tracks', 2], 'Track 3'),
			Field(['tracks', 3], 'Track 4'),
			Field(['tracks', 4], 'Track 5'),
			])

	def testToTheMiddleNestedList(self):
		"""Test insertion to the middle of nested list"""
		self.prepareStandNestedList()

		res = self.db.processRequest(InsertRequest('2',
			Field(['tracks', 1]),
			[
				Field(['Name'], 'Track 4 name'),
				Field(['Name'], 'Track 5 name')
			]
		))

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Name'])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Name'], 'Track 1 name'),
			Field(['tracks', 1, 'Name'], 'Track 4 name'),
			Field(['tracks', 2, 'Name'], 'Track 5 name'),
			Field(['tracks', 3, 'Name'], 'Track 2 name'),
			Field(['tracks', 4, 'Name'], 'Track 3 name'),
			])

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Authors', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Authors', 0], 'Carl II'),
			Field(['tracks', 0, 'Authors', 1], 'Dan'),
			Field(['tracks', 3, 'Authors', 0], 'Alex'),
			Field(['tracks', 4, 'Authors', 0], 'Rob')
			])

	def testToTheBeginningNestedList(self):
		"""Test insertion to the beginning of nested list"""
		self.prepareStandNestedList()

		res = self.db.processRequest(InsertRequest('2',
			Field(['tracks', 0]),
			[
				Field(['Name'], 'Track 4 name'),
				Field(['Name'], 'Track 5 name')
			]
		))

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Name'])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Name'], 'Track 4 name'),
			Field(['tracks', 1, 'Name'], 'Track 5 name'),
			Field(['tracks', 2, 'Name'], 'Track 1 name'),
			Field(['tracks', 3, 'Name'], 'Track 2 name'),
			Field(['tracks', 4, 'Name'], 'Track 3 name'),
			])

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Authors', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 2, 'Authors', 0], 'Carl II'),
			Field(['tracks', 2, 'Authors', 1], 'Dan'),
			Field(['tracks', 3, 'Authors', 0], 'Alex'),
			Field(['tracks', 4, 'Authors', 0], 'Rob')
			])

	def testToTheEndNestedList(self):
		"""Test insertion to the end of nested list"""
		self.prepareStandNestedList()

		res = self.db.processRequest(InsertRequest('2',
			Field(['tracks', None]),
			[
				Field(['Name'], 'Track 4 name'),
				Field(['Name'], 'Track 5 name')
			]
		))

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Name'])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Name'], 'Track 1 name'),
			Field(['tracks', 1, 'Name'], 'Track 2 name'),
			Field(['tracks', 2, 'Name'], 'Track 3 name'),
			Field(['tracks', 3, 'Name'], 'Track 4 name'),
			Field(['tracks', 4, 'Name'], 'Track 5 name'),
			])

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Authors', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Authors', 0], 'Carl II'),
			Field(['tracks', 0, 'Authors', 1], 'Dan'),
			Field(['tracks', 1, 'Authors', 0], 'Alex'),
			Field(['tracks', 2, 'Authors', 0], 'Rob')
			])

	def testToTheBeginningNestedListOnePosition(self):
		"""Test insertion to the beginning of nested list, one position shift"""
		self.prepareStandNestedList()

		res = self.db.processRequest(InsertRequest('2',
			Field(['tracks', 0]),
			[
				Field(['Authors', 0], 'Earl'),
				Field(['Authors', 1], 'Fred')
			],
			True
		))

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Name'])]))
		self.checkRequestResult(res, [
			Field(['tracks', 1, 'Name'], 'Track 1 name'),
			Field(['tracks', 2, 'Name'], 'Track 2 name'),
			Field(['tracks', 3, 'Name'], 'Track 3 name'),
		])

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Authors', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Authors', 0], 'Earl'),
			Field(['tracks', 0, 'Authors', 1], 'Fred'),
			Field(['tracks', 1, 'Authors', 0], 'Carl II'),
			Field(['tracks', 1, 'Authors', 1], 'Dan'),
			Field(['tracks', 2, 'Authors', 0], 'Alex'),
			Field(['tracks', 3, 'Authors', 0], 'Rob')
		])

	def testToTheEndNestedListOnePosition(self):
		"""Test insertion to the end of nested list, one position shift"""
		self.prepareStandNestedList()

		res = self.db.processRequest(InsertRequest('2',
			Field(['tracks', None]),
			[
				Field(['Authors', 0], 'Earl'),
				Field(['Authors', 1], 'Fred')
			],
			True
		))

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Name'])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Name'], 'Track 1 name'),
			Field(['tracks', 1, 'Name'], 'Track 2 name'),
			Field(['tracks', 2, 'Name'], 'Track 3 name')
		])

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Authors', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Authors', 0], 'Carl II'),
			Field(['tracks', 0, 'Authors', 1], 'Dan'),
			Field(['tracks', 1, 'Authors', 0], 'Alex'),
			Field(['tracks', 2, 'Authors', 0], 'Rob'),
			Field(['tracks', 3, 'Authors', 0], 'Earl'),
			Field(['tracks', 3, 'Authors', 1], 'Fred')
		])

	def testToTheEndSeveralLists(self):
		"""Test insertion to the end of list when there are other lists on the same level"""
		self.prepareStandNestedList()

		res = self.db.processRequest(InsertRequest('2',
			Field(['tracks', 1, 'Authors', None]),
			[
				Field([], 'Yngwie'),
				Field([], 'Zack')
			]
		))

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Authors', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Authors', 0], 'Carl II'),
			Field(['tracks', 0, 'Authors', 1], 'Dan'),
			Field(['tracks', 1, 'Authors', 0], 'Alex'),
			Field(['tracks', 1, 'Authors', 1], 'Yngwie'),
			Field(['tracks', 1, 'Authors', 2], 'Zack'),
			Field(['tracks', 2, 'Authors', 0], 'Rob')
			])

	def testToEmptyList(self):
		"""Check that insertion to non-existing list creates this list"""
		self.prepareStandSimpleList()

		res = self.db.processRequest(InsertRequest('2',
			Field(['tracks', 2, 'Authors', None]),
			[
				Field([], 'Earl'),
				Field([], 'Fred')
			]
		))

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Authors', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 2, 'Authors', 0], 'Earl'),
			Field(['tracks', 2, 'Authors', 1], 'Fred')
		])

	def testToNonExistingObject(self):
		"""Check that insertion to non-existing object creates object first"""
		self.prepareStandSimpleList()

		res = self.db.processRequest(InsertRequest('3',
			Field(['tracks', 2, 'Authors', None]),
			[
				Field([], 'Earl'),
				Field([], 'Fred')
			]
		))

		res = self.db.processRequest(ReadRequest('3', [Field(['tracks', None, 'Authors', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 2, 'Authors', 0], 'Earl'),
			Field(['tracks', 2, 'Authors', 1], 'Fred')
		])

	def testToListWithSeveralTypes(self):
		"""
		Check that several values of different types can be inserted
		to list which already has values of several types
		"""
		self.prepareStandDifferentTypes()

		res = self.db.processRequest(InsertRequest('1',
			Field(['meta', 2]),
			[
				Field([], 'Monk'),
				Field([], 2),
				Field([], 10.0)
			]
		))

		res = self.db.processRequest(ReadRequest('1', [Field(['meta', None])]))

		self.checkRequestResult(res, [
			Field(['meta', 0], 'Pikeman'),
			Field(['meta', 1], 'Archer'),
			Field(['meta', 2], 'Monk'),
			Field(['meta', 3], 2),
			Field(['meta', 4], 10.0),
			Field(['meta', 5], 1),
			Field(['meta', 6], 2),
			Field(['meta', 7], 4.0),
			Field(['meta', 8], 5.0),
			Field(['meta', 9], b'Gryphon'),
			Field(['meta', 10], b'Swordsman')
		])

def get_class():
	return Insert