"""Unit tests for Delete requests"""

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import unittest
from test import helpers
from test.db.requests import TestRequest
from db.database import *
from db.interface import *

class Delete(TestRequest):
	"""Test operation of DeleteRequest"""

	def testWholeObject(self):
		"""Check deletion of the whole object"""
		self.prepareStandNoList()

		self.db.processRequest(DeleteRequest('3'))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '3333')))
		self.checkRequestResult(res, [])

	def testWholeObjectPreservesOthers(self):
		"""Check that deletion of the whole object does not spoil the database"""
		self.prepareStandNoList()

		self.db.processRequest(DeleteRequest('3'))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '1111')))
		self.checkRequestResult(res, ['1', '5'])

	def testExistentFieldsPreservesOtherFiels(self):
		"""Check that deletion of existent fields preserves other object fields"""
		self.prepareStandNoList()

		self.db.processRequest(DeleteRequest('3', [Field('age'), Field('phone')]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('name'), SearchRequest.Eq(), 'Carl')))
		self.checkRequestResult(res, ['3'])

	def testExistentFieldsReallyDeleted(self):
		"""Check that deletion of existent fields actually deletes them"""
		self.prepareStandNoList()

		self.db.processRequest(DeleteRequest('3', [Field('age'), Field('phone')]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '3333')))
		self.checkRequestResult(res, [])

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('age'), SearchRequest.Eq(), '27')))
		self.checkRequestResult(res, [])

	def testNonExistentFields(self):
		"""Check deletion of non-existent fields"""
		self.prepareStandNoList()

		self.db.processRequest(DeleteRequest('2', [Field('name'), Field('blablabla')]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('name'), SearchRequest.Eq(), 'Bob')))
		self.checkRequestResult(res, [])

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '2222')))
		self.checkRequestResult(res, ['2'])

	def testAllElements(self):
		"""Test that deleting all elements does not spoil the database"""
		self.prepareStandNoList()

		# Remove all
		self.db.processRequest(DeleteRequest('1'))
		self.db.processRequest(DeleteRequest('2'))
		self.db.processRequest(DeleteRequest('3'))

		# Add object again
		self.addObject('2', {'name': 'Alex', 'phone': '2222'})

		# Check that addition was successful
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '2222')))

		self.checkRequestResult(res, ['2'])

	def testSimpleListFromMiddle(self):
		"""Test deletion from the middle of the list"""
		self.prepareStandSimpleList()

		self.db.processRequest(DeleteRequest('1', [
			Field(['tracks', 1])
		]))

		res = self.db.processRequest(ReadRequest('1', [Field(['tracks', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0], 'Track 1'),
			Field(['tracks', 1], 'Track 3')
			])

	def testSimpleListFromBeginning(self):
		"""Test deletion from the beginning of the list"""
		self.prepareStandSimpleList()

		self.db.processRequest(DeleteRequest('1', [
			Field(['tracks', 0])
		]))

		res = self.db.processRequest(ReadRequest('1', [Field(['tracks', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0], 'Track 2'),
			Field(['tracks', 1], 'Track 3')
			])

	def testSimpleListFromEnd(self):
		"""Test deletion from the end of the list"""
		self.prepareStandSimpleList()

		self.db.processRequest(DeleteRequest('1', [
			Field(['tracks', 2])
		]))

		res = self.db.processRequest(ReadRequest('1', [Field(['tracks', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0], 'Track 1'),
			Field(['tracks', 1], 'Track 2')
			])

	def testNestedListFromMiddle(self):
		"""Test deletion from the middle of the nested list"""
		self.prepareStandNestedList()

		self.db.processRequest(DeleteRequest('2', [
			Field(['tracks', 1])
		]))

		# Check that deletion really occurred
		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0], 'Track 11'),
			Field(['tracks', 1], 'Track 3')
			])

		# Check that reenumeration occurred
		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Name'])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Name'], 'Track 1 name'),
			Field(['tracks', 1, 'Name'], 'Track 3 name')
			])

	def testNestedListFromBeginning(self):
		"""Test deletion from the beginning of the nested list"""
		self.prepareStandNestedList()

		self.db.processRequest(DeleteRequest('2', [
			Field(['tracks', 0])
		]))

		# Check that deletion really occurred
		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0], 'Track 2'),
			Field(['tracks', 1], 'Track 3')
			])

		# Check that reenumeration occurred
		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Name'])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Name'], 'Track 2 name'),
			Field(['tracks', 1, 'Name'], 'Track 3 name')
			])

	def testNestedListFromEnd(self):
		"""Test deletion from the end of the nested list"""
		self.prepareStandNestedList()

		self.db.processRequest(DeleteRequest('2', [
			Field(['tracks', 2])
		]))

		# Check that deletion really occurred
		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0], 'Track 11'),
			Field(['tracks', 1], 'Track 2')
			])

		# Check that reenumeration occurred
		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Name'])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Name'], 'Track 1 name'),
			Field(['tracks', 1, 'Name'], 'Track 2 name')
			])

def get_class():
	return Delete
