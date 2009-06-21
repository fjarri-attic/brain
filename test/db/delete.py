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

		# check that field of deleted object is gone
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.EQ, '3333')))
		self.checkRequestResult(res, [])

		# Check that other objects are intact
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.EQ, '1111')))
		self.checkRequestResult(res, ['1', '5'])

	def testExistentFields(self):
		"""Check that deletion of existent fields preserves other object fields"""
		self.prepareStandNoList()

		self.db.processRequest(DeleteRequest('3', [Field('age'), Field('phone')]))

		# Check that other fields are intact
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('name'), SearchRequest.EQ, 'Carl')))
		self.checkRequestResult(res, ['3'])

		# Check that fields were really deleted
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.EQ, '3333')))
		self.checkRequestResult(res, [])

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('age'), SearchRequest.EQ, '27')))
		self.checkRequestResult(res, [])

	def testNonExistentFields(self):
		"""Check deletion of non-existent fields"""
		self.prepareStandNoList()

		self.db.processRequest(DeleteRequest('2', [Field('name'), Field('blablabla')]))

		# Check that existent field was deleted
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('name'), SearchRequest.EQ, 'Bob')))
		self.checkRequestResult(res, [])

		# Check that other fields are intact
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.EQ, '2222')))
		self.checkRequestResult(res, ['2'])

	def testAllObjects(self):
		"""Test that deleting all objects does not spoil the database"""
		self.prepareStandNoList()

		# Remove all
		self.db.processRequest(DeleteRequest('1'))
		self.db.processRequest(DeleteRequest('2'))
		self.db.processRequest(DeleteRequest('3'))
		self.db.processRequest(DeleteRequest('4'))
		self.db.processRequest(DeleteRequest('5'))

		# Add object again
		self.addObject('2', {'name': 'Alex', 'phone': '2222'})

		# Check that addition was successful
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field('phone'), SearchRequest.EQ, '2222')))

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

		# Check that deletion and reenumeration occurred
		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Name'])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Name'], 'Track 1 name'),
			Field(['tracks', 1, 'Name'], 'Track 3 name')
		])

		# Check that nested list is intact and reenumeration occurred in it too
		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Authors', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Authors', 0], 'Carl II'),
			Field(['tracks', 0, 'Authors', 1], 'Dan'),
			Field(['tracks', 1, 'Authors', 0], 'Rob')
		])

	def testNestedListFromBeginning(self):
		"""Test deletion from the beginning of the nested list"""
		self.prepareStandNestedList()

		self.db.processRequest(DeleteRequest('2', [
			Field(['tracks', 0])
		]))

		# Check that deletion and reenumeration occurred
		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Name'])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Name'], 'Track 2 name'),
			Field(['tracks', 1, 'Name'], 'Track 3 name')
		])

		# Check that nested list is intact and reenumeration occurred in it too
		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Authors', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Authors', 0], 'Alex'),
			Field(['tracks', 1, 'Authors', 0], 'Rob')
		])

	def testNestedListFromEnd(self):
		"""Test deletion from the end of the nested list"""
		self.prepareStandNestedList()

		self.db.processRequest(DeleteRequest('2', [
			Field(['tracks', 2])
		]))

		# Check that deletion occurred
		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Name'])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Name'], 'Track 1 name'),
			Field(['tracks', 1, 'Name'], 'Track 2 name')
		])

		# Check that nested list is intact
		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Authors', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Authors', 0], 'Carl II'),
			Field(['tracks', 0, 'Authors', 1], 'Dan'),
			Field(['tracks', 1, 'Authors', 0], 'Alex')
		])

	def testFromListByMaskLeaf(self):
		"""Test deletion using list mask, leaf list"""
		self.prepareStandNestedList()

		self.db.processRequest(DeleteRequest('2', [
			Field(['tracks', 0, 'Authors', None])
		]))

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Authors', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 1, 'Authors', 0], 'Alex'),
			Field(['tracks', 2, 'Authors', 0], 'Rob')
		])

	def testFromListByMask(self):
		"""Test deletion using list mask, non-leaf list"""
		self.prepareStandNestedList()

		self.db.processRequest(DeleteRequest('2', [
			Field(['tracks', None, 'Authors', 0])
		]))

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Authors', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Authors', 0], 'Dan')
		])

	def testFromListKeepsNeighbors(self):
		"""
		Regression test for bug when deleting element from list deletes all neighbors
		from its level of hierarchy
		"""
		self.prepareStandNestedList()

		self.db.processRequest(DeleteRequest('2', [
			Field(['tracks', 0, 'Authors', 0])
		]))

		res = self.db.processRequest(ReadRequest('2', [Field(['tracks', None, 'Authors', 0])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Authors', 0], 'Dan'),
			Field(['tracks', 1, 'Authors', 0], 'Alex'),
			Field(['tracks', 2, 'Authors', 0], 'Rob')
		])

	def testAllValuesFromField(self):
		"""Check that after we delete all values from some field of all objects, database is not spoiled"""
		self.prepareStandNoList()

		self.db.processRequest(DeleteRequest('3', [
			Field(['age'])
		]))

		self.db.processRequest(DeleteRequest('4', [
			Field(['age'])
		]))

		self.db.processRequest(DeleteRequest('5', [
			Field(['age'])
		]))

		# check that searching in this table does not produce errors
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['age']), SearchRequest.EQ, '22')))
		self.checkRequestResult(res, [])

		# check that we can create this table back
		self.db.processRequest(ModifyRequest('3', [
			Field(['age'], '22')
		]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['age']), SearchRequest.EQ, '22')))
		self.checkRequestResult(res, ['3'])

	def testEmptyTableAfterRenumbering(self):
		"""
		Check that the situation when all values are deleted from some field after renumbering
		does not break the database
		"""
		self.prepareStandNestedList()

		self.db.processRequest(DeleteRequest('1', [
			Field(['tracks', 2])
		]))

		# check that value was deleted
		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', None, 'Lyrics', None]), SearchRequest.EQ, 'Lalala')))
		self.checkRequestResult(res, [])

		# add this value back
		self.db.processRequest(ModifyRequest('1', [
			Field(['tracks', 2, 'Lyrics', 0], 'Blablabla')
		]))

		res = self.db.processRequest(SearchRequest(SearchRequest.Condition(
			Field(['tracks', None, 'Lyrics', None]), SearchRequest.EQ, 'Blablabla')))
		self.checkRequestResult(res, ['1'])

	def testNonExistingObject(self):
		"""Check that deletion of non-existing object does not raise anything"""
		self.prepareStandNoList()

		self.db.processRequest(DeleteRequest('6'))

	def testSeveralTypesAtOnce(self):
		"""Check that values of different types can be deleted at once by mask"""
		self.prepareStandDifferentTypes()

		self.db.processRequest(DeleteRequest('1', [
			Field(['meta', None])
		]))

		res = self.db.processRequest(ReadRequest('1', [Field(['meta', None])]))

		self.checkRequestResult(res, [])

	def testNoneValue(self):
		"""Check that Null values can be deleted"""
		self.db.processRequest(ModifyRequest('1', [
			Field(['fld1', 0], value=None),
			Field(['fld1', 1], value=1)
		]))

		self.db.processRequest(DeleteRequest('1', [
			Field(['fld1', 0])
		]))

		res = self.db.processRequest(ReadRequest('1'))

		self.checkRequestResult(res, [
			Field(['fld1', 0], value=1)
			])

	def testListInTheMiddle(self):
		"""Regression test for incorrect _InternalField.pointsToListElement() work"""
		self.prepareStandNestedList()

		# here pointsToListElements() returned true, because the last list index
		# is defined; but it is not the last name element, so the field really
		# does not point to list
		self.db.processRequest(DeleteRequest('1', [
			Field(['tracks', 0, 'Name'])
		]))

		res = self.db.processRequest(ReadRequest('1', [Field(['tracks', None, 'Name'])]))
		self.checkRequestResult(res, [
			Field(['tracks', 1, 'Name'], value='Track 2 name')
		])

def get_class():
	return Delete
