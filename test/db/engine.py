"""Unit tests for database enginr layer"""

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import unittest
from test import helpers
from db.engine import *

def getParameterized(base_class, name_prefix, db_class, db_file_name):
	"""Get named test suite with predefined setUp()"""
	class Derived(base_class):
		def setUp(self):
			self.db = db_class(db_file_name)

	Derived.__name__ = name_prefix + "." + base_class.__name__

	return Derived

class EngineTest(unittest.TestCase):

	def testValueTransformation(self):
		vals = ['a', 'b b']

		for val in vals:
			safe_val = self.db.getSafeValueFromString(val)
			unsafe_val = self.db.getStringFromSafeValue(safe_val)
			self.failUnlessEqual(val, unsafe_val)

	def testTableNameTransformation(self):
		names = [
			(['a'], None),
			(['a', 'b'], None),
			(['b', None, None], None),
			(['b', 1, 0], ['b', None, None])
		]

		for name, expected_res in names:
			if expected_res == None:
				expected_res = name

			safe_name = self.db.getSafeTableNameFromList(name)
			unsafe_name = self.db.getListFromSafeTableName(safe_name)
			self.failUnlessEqual(expected_res, unsafe_name)


def suite():
	"""Generate test suite for this module"""
	res = helpers.NamedTestSuite()

	parameters = [
		('memory.sqlite3', Sqlite3Engine, ':memory:'),
	]

	classes = [EngineTest]

	for parameter in parameters:
		for c in classes:
			res.addTest(unittest.TestLoader().loadTestsFromTestCase(
				getParameterized(c, *parameter)))

	return res
