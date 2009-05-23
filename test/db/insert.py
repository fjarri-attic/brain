"""Unit tests for database layer insert request"""

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import unittest
from test import helpers
from test.db.requests_base import TestRequest
from db.database import *
from db.interface import *

class TestInsertRequest(TestRequest):
	"""Test operation of InsertRequest"""

	def testToTheMiddleSimpleList(self):
		"""Check insertion to the middle of simple list"""
		self.prepareStandSimpleList()

		res = self.db.processRequest(InsertRequest('1',
			Field(['tracks', 1]),
			[
				Field(['tracks', None], 'Track 4'),
				Field(['tracks', None], 'Track 5')
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
				Field(['tracks', None], 'Track 4'),
				Field(['tracks', None], 'Track 5')
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
				Field(['tracks', None], 'Track 4'),
				Field(['tracks', None], 'Track 5')
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
				Field(['tracks', None], 'Track 4'),
				Field(['tracks', None], 'Track 5')
			]
		))

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0], 'Track 11'),
			Field(['tracks', 1], 'Track 4'),
			Field(['tracks', 2], 'Track 5'),
			Field(['tracks', 3], 'Track 2'),
			Field(['tracks', 4], 'Track 3'),
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
				Field(['tracks', None], 'Track 4'),
				Field(['tracks', None], 'Track 5')
			]
		))

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0], 'Track 4'),
			Field(['tracks', 1], 'Track 5'),
			Field(['tracks', 2], 'Track 11'),
			Field(['tracks', 3], 'Track 2'),
			Field(['tracks', 4], 'Track 3'),
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
				Field(['tracks', None], 'Track 4'),
				Field(['tracks', None], 'Track 5')
			]
		))

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0], 'Track 11'),
			Field(['tracks', 1], 'Track 2'),
			Field(['tracks', 2], 'Track 3'),
			Field(['tracks', 3], 'Track 4'),
			Field(['tracks', 4], 'Track 5'),
			])

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Authors', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Authors', 0], 'Carl II'),
			Field(['tracks', 0, 'Authors', 1], 'Dan'),
			Field(['tracks', 1, 'Authors', 0], 'Alex'),
			Field(['tracks', 2, 'Authors', 0], 'Rob')
			])

def get_class():
	return TestInsertRequest
