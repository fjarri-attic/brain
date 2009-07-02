"""Unit tests for database layer read request"""

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import unittest
from test import helpers
from test.internal.requests import TestRequest
from brain.database import *
from brain.interface import *

class Read(TestRequest):
	"""Test operation of ReadRequest"""

	def testAllFields(self):
		"""Check the operation of whole object reading"""
		self.prepareStandNoList()

		res = self.db.processRequest(ReadRequest(self.id1))
		self.checkRequestResult(res, [
			Field('name', 'Alex'),
			Field('phone', '1111')])

	def testSomeFields(self):
		"""Check the operation of reading some chosen fields"""
		self.prepareStandNoList()

		res = self.db.processRequest(ReadRequest(self.id1, [Field('name')]))
		self.checkRequestResult(res, [
			Field('name', 'Alex'),
			])

	def testNonExistingField(self):
		"""Check that non-existent field is ignored during read"""
		self.prepareStandNoList()

		res = self.db.processRequest(ReadRequest(self.id1, [Field('name'), Field('age')]))
		self.checkRequestResult(res, [
			Field('name', 'Alex')
			])

	def testAddedList(self):
		"""Check that list values can be read"""
		self.prepareStandSimpleList()

		res = self.db.processRequest(ReadRequest(self.id1, [Field(['tracks', None])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0], 'Track 1'),
			Field(['tracks', 1], 'Track 2'),
			Field(['tracks', 2], 'Track 3')
			])

	def testAddedListComplexCondition(self):
		"""Check that read request works properly when some list positions are defined"""
		self.prepareStandNestedList()

		res = self.db.processRequest(ReadRequest(self.id1, [Field(['tracks', None, 'Authors', 0])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Authors', 0], 'Alex'),
			Field(['tracks', 1, 'Authors', 0], 'Carl I')
			])

	def testFromMiddleLevelList(self):
		"""Check that one can read from list in the middle of the hierarchy"""
		self.prepareStandNestedList()

		res = self.db.processRequest(ReadRequest(self.id1, [Field(['tracks', None, 'Name'])]))
		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Name'], 'Track 1 name'),
			Field(['tracks', 1, 'Name'], 'Track 2 name')
			])

	def testNonExistingObject(self):
		"""Check that read request for non-existing object raises error"""
		self.prepareStandNoList()

		self.failUnlessRaises(interface.LogicError, self.db.processRequest, ReadRequest('6'))

	def testSeveralTypesAtOnce(self):
		"""Check that different values of types can be read at once by mask"""
		self.prepareStandDifferentTypes()

		res = self.db.processRequest(ReadRequest(self.id1, [Field(['meta', None])]))

		self.checkRequestResult(res, [
			Field(['meta', 0], 'Pikeman'),
			Field(['meta', 1], 'Archer'),
			Field(['meta', 2], 1),
			Field(['meta', 3], 2),
			Field(['meta', 4], 4.0),
			Field(['meta', 5], 5.0),
			Field(['meta', 6], b'Gryphon'),
			Field(['meta', 7], b'Swordsman')
		])

	def testSubTree(self):
		"""Check that subtree can be read at once"""
		self.prepareStandNestedList()

		res = self.db.processRequest(ReadRequest(self.id1, [Field(['tracks', 0, 'Authors'])]))

		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Authors', 0], 'Alex'),
			Field(['tracks', 0, 'Authors', 1], 'Bob')
		])

	def testSubTreesByMask(self):
		"""Check that several subtrees can be read at once by mask"""
		self.prepareStandNestedList()

		res = self.db.processRequest(ReadRequest(self.id1, [Field(['tracks', None, 'Authors'])]))

		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Authors', 0], 'Alex'),
			Field(['tracks', 0, 'Authors', 1], 'Bob'),
			Field(['tracks', 1, 'Authors', 0], 'Carl I')
		])

	def testComplexStructures(self):
		"""Check that several complex structures can be read at once"""
		self.prepareStandDifferentTypes()

		res = self.db.processRequest(ReadRequest(self.id1, [
			Field(['tracks', 0]),
			Field(['meta'])
		]))

		self.checkRequestResult(res, [
			Field(['tracks', 0, 'Name'], 'Track 1 name'),
			Field(['tracks', 0, 'Length'], 300),
			Field(['tracks', 0, 'Volume'], 29.4),
			Field(['tracks', 0, 'Authors', 0], 'Alex'),
			Field(['tracks', 0, 'Authors', 1], 'Bob'),
			Field(['tracks', 0, 'Data'], b'\x00\x01\x02'),
			Field(['meta', 0], 'Pikeman'),
			Field(['meta', 1], 'Archer'),
			Field(['meta', 2], 1),
			Field(['meta', 3], 2),
			Field(['meta', 4], 4.0),
			Field(['meta', 5], 5.0),
			Field(['meta', 6], b'Gryphon'),
			Field(['meta', 7], b'Swordsman')
		])


def get_class():
	return Read
