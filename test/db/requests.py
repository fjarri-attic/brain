"""Unit with basic mechanisms of database layer requests testing"""

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import unittest
from db.interface import *

def _compareLists(l1, l2):
	"""Check if all elements of the first list exist in the second list"""
	for elem in l1:
		if elem in l2:
			l2.remove(elem)
		else:
			raise Exception("Cannot find " + str(elem) + " in second list")

def getParameterized(base_class, name_prefix, db_class, *params):
	"""Get named test suite with predefined setUp()"""
	class Derived(base_class):
		def setUp(self):
			self.db = db_class(*params)

	Derived.__name__ = name_prefix + "." + base_class.__name__

	return Derived

class TestRequest(unittest.TestCase):
	"""Base class for database requests testing"""

	def setUp(self):
		"""Stub for setUp() so that nobody creates the instance of this class"""
		raise Exception("Not implemented")

	def checkRequestResult(self, res, expected):
		"""Compare request results with expected list"""
		self.failUnless(isinstance(res, list), "Request result has type " + str(type(res)))
		self.failUnless(len(res) == len(expected), "Request returned " + str(len(res)) + " results")
		_compareLists(res, expected)

	def addObject(self, id, fields={}):
		"""Add object with given fields to database"""
		field_objs = [Field(key, fields[key]) for key in fields.keys()]
		self.db.processRequest(ModifyRequest(id, field_objs))

	def prepareStandNoList(self):
		"""Prepare DB wiht several objects which contain only hashes"""
		self.addObject('1', {'name': 'Alex', 'phone': '1111'})
		self.addObject('2', {'name': 'Bob', 'phone': '2222'})
		self.addObject('3', {'name': 'Carl', 'phone': '3333', 'age': '27'})
		self.addObject('4', {'name': 'Don', 'phone': '4444', 'age': '20'})
		self.addObject('5', {'name': 'Alex', 'phone': '1111', 'age': '22'})

	def prepareStandSimpleList(self):
		"""Prepare DB with several objects which contain simple lists"""
		self.db.processRequest(ModifyRequest('1', [
			Field(['tracks', 0], value='Track 1'),
			Field(['tracks', 1], value='Track 2'),
			Field(['tracks', 2], value='Track 3')]
			))
		self.db.processRequest(ModifyRequest('2', [
			Field(['tracks', 0], value='Track 2'),
			Field(['tracks', 1], value='Track 1')]
			))

	def prepareStandNestedList(self):
		"""Prepare DB with several objects which contain nested lists"""
		self.db.processRequest(ModifyRequest('1', [
			Field(['tracks', 0, 'Name'], value='Track 1 name'),
			Field(['tracks', 0, 'Length'], value='Track 1 length'),
			Field(['tracks', 0, 'Authors', 0], value='Alex'),
			Field(['tracks', 0, 'Authors', 1], value='Bob'),

			Field(['tracks', 1, 'Name'], value='Track 2 name'),
			Field(['tracks', 1, 'Authors', 0], value='Carl I'),

			Field(['tracks', 2, 'Lyrics', 0], value='Lalala')
			]))

		self.db.processRequest(ModifyRequest('2', [
			Field(['tracks', 0, 'Name'], value='Track 1 name'),
			Field(['tracks', 0, 'Length'], value='Track 1 length'),
			Field(['tracks', 0, 'Authors', 0], value='Carl II'),
			Field(['tracks', 0, 'Authors', 1], value='Dan'),

			Field(['tracks', 1, 'Name'], value='Track 2 name'),
			Field(['tracks', 1, 'Authors', 0], value='Alex'),

			Field(['tracks', 2, 'Name'], value='Track 3 name'),
			Field(['tracks', 2, 'Authors', 0], value='Rob'),
			]))

	def prepareStandDifferentTypes(self):
		"""Prepare DB with several objects and different value types"""
		self.db.processRequest(ModifyRequest('1', [
			Field(['name'], 'Album 1'),

			Field(['tracks', 0, 'Name'], 'Track 1 name'),
			Field(['tracks', 0, 'Length'], 300),
			Field(['tracks', 0, 'Volume'], 29.4),
			Field(['tracks', 0, 'Authors', 0], 'Alex'),
			Field(['tracks', 0, 'Authors', 1], 'Bob'),
			Field(['tracks', 0, 'Data'], b'\x00\x01\x02'),

			Field(['tracks', 1, 'Name'], 'Track 2 name'),
			Field(['tracks', 1, 'Length'], 350),
			Field(['tracks', 1, 'Volume'], 26.5),
			Field(['tracks', 1, 'Rating'], 4),
			Field(['tracks', 1, 'Authors', 0], 'Carl'),
			Field(['tracks', 1, 'Authors', 1], 'Dan'),
			Field(['tracks', 1, 'Data'], b'\x00\x01\x03'),

			Field(['meta', 0], 'Pikeman'),
			Field(['meta', 1], 'Archer'),
			Field(['meta', 2], 1),
			Field(['meta', 3], 2),
			Field(['meta', 4], 4.0),
			Field(['meta', 5], 5.0),
			Field(['meta', 6], b'Gryphon'),
			Field(['meta', 7], b'Swordsman')
			]))

		self.db.processRequest(ModifyRequest('2', [
			Field(['name'], 'Album 2'),

			Field(['tracks', 0, 'Name'], 'Track 3 name'),
			Field(['tracks', 0, 'Length'], 290),
			Field(['tracks', 0, 'Volume'], 33.0),
			Field(['tracks', 0, 'Authors', 0], 'Earl'),
			Field(['tracks', 0, 'Authors', 1], 'Fred'),
			Field(['tracks', 0, 'Authors', 2], 'Greg'),
			Field(['tracks', 0, 'Data'], b'\x00\x01\x04'),

			Field(['tracks', 1, 'Name'], 'Track 4 name'),
			Field(['tracks', 1, 'Length'], 370),
			Field(['tracks', 1, 'Volume'], 22.1),
			Field(['tracks', 1, 'Rating'], 4),
			Field(['tracks', 1, 'Authors', 0], 'Hugo'),
			Field(['tracks', 1, 'Data'], b'\x00\x01\x05')
			]))
