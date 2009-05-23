"""Unit tests for database layer read request"""

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import unittest
from test import helpers
from test.db.requests_base import TestRequest
from db.database import *
from db.interface import *

class TestReadRequest(TestRequest):
	"""Test operation of ReadRequest"""

	def testAllFields(self):
		"""Check the operation of whole object reading"""
		self.prepareStandNoList()

		res = self.db.processRequest(ReadRequest('1'))
		self.checkRequestResult(res, [
			Field('name', 'Alex'),
			Field('phone', '1111')])

	def testSomeFields(self):
		"""Check the operation of reading some chosen fields"""
		self.prepareStandNoList()

		res = self.db.processRequest(ReadRequest('1', [Field('name')]))
		self.checkRequestResult(res, [
			Field('name', 'Alex'),
			])

	def testNonExistingField(self):
		"""Check that non-existent field is ignored during read"""
		self.prepareStandNoList()

		res = self.db.processRequest(ReadRequest('1', [Field('name'), Field('age')]))
		self.checkRequestResult(res, [
			Field('name', 'Alex')
			])

	def testAddedList(self):
		"""Check that list values can be read"""
		self.prepareStandSimpleList()

		res = self.db.processRequest(ReadRequest('1', [Field(['tracks', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0], 'Track 1'),
			Field(['tracks', 1], 'Track 2'),
			Field(['tracks', 2], 'Track 3')
			])

	def testAddedListComplexCondition(self):
		"""Check that read request works properly when some list positions are defined"""
		self.prepareStandNestedList()

		res = self.db.processRequest(ReadRequest('1', [Field(['tracks', None, 'Authors', 0])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Authors', 0], 'Alex'),
			Field(['tracks', 1, 'Authors', 0], 'Carl I')
			])

	def testFromMiddleLevelList(self):
		"""Check that one can read from list in the middle of the hierarchy"""
		self.prepareStandNestedList()

		res = self.db.processRequest(ReadRequest('1', [Field(['tracks', None, 'Name'])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Name'], 'Track 1 name'),
			Field(['tracks', 1, 'Name'], 'Track 2 name')
			])

def get_class():
	return TestReadRequest
