"""Unit tests for database layer modify request"""

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import unittest
from test import helpers
from test.db.requests import TestRequest
from db.database import *
from db.interface import *

class Modify(TestRequest):
	"""Test different uses of ModifyRequest"""

	def testBlankObjectAddition(self):
		"""Check that object without fields can be created"""
		self.addObject('1')

	def testAdditionNoCheck(self):
		"""Check simple object addition"""
		self.prepareStandNoList()

	def testAddition(self):
		"""Simple object addition with result checking"""
		self.prepareStandNoList()

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '1111')))

		self.checkRequestResult(res, ['1', '5'])

	def testAdditionSameFields(self):
		"""Check that several equal fields are handled correctly"""
		self.db.processRequest(ModifyRequest('1', [
			Field(['tracks'], value='Track 1'),
			Field(['tracks'], value='Track 2'),
			Field(['tracks'], value='Track 3')]
			))

		res = self.db.processRequest(ReadRequest('1', [Field(['tracks'])]))

		# only last field value should be saved
		self.checkRequestResult(res, [
			Field(['tracks'], 'Track 3')
			])

	def testModificationNoCheck(self):
		"""Check object modification"""
		self.prepareStandNoList()

		self.db.processRequest(ModifyRequest('1', [
			Field('name', 'Zack')
		]))

	def testModification(self):
		"""Object modification with results checking"""
		self.prepareStandNoList()

		self.db.processRequest(ModifyRequest('1', [
			Field('name', 'Zack')
		]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('name'), SearchRequest.Eq(), 'Zack')))

		self.checkRequestResult(res, ['1'])

	def testModificationAddsField(self):
		"""Check that object modification can add a new field"""
		self.prepareStandNoList()

		self.db.processRequest(ModifyRequest('1', [
			Field('age', '66')
		]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('age'), SearchRequest.Eq(), '66')))

		self.checkRequestResult(res, ['1'])

	def testModificationAddsFieldTwice(self):
		"""Regression test for non-updating specification"""
		self.prepareStandNoList()

		# Add new field to object
		self.db.processRequest(ModifyRequest('1', [
			Field('age', '66')
		]))

		# Delete object. If specification was not updated,
		# new field is still in database
		self.db.processRequest(DeleteRequest('1'))

		# Add object again
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})

		# Check that field from old object is not there
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('age'), SearchRequest.Eq(), '66')))

		self.checkRequestResult(res, [])

	def testModificationPreservesFields(self):
		"""Check that modification preserves existing fields"""
		self.prepareStandNoList()

		self.db.processRequest(ModifyRequest('2', [
			Field('name', 'Zack')
		]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.Eq(), '2222')))

		self.checkRequestResult(res, ['2'])

	def testListAdditions(self):
		"""Regression test for erroneous modify results for lists"""
		self.prepareStandSimpleList()

		res = self.db.processRequest(ModifyRequest('1',
			[
				Field(['tracks', 3], 'Track 4'),
				Field(['tracks', 4], 'Track 5')
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

def get_class():
	return Modify
