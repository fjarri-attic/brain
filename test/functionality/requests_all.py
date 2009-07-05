"""Unit tests for database layer requests"""

import unittest

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import brain
import helpers
from functionality import delete, insert, modify, read, search, requests, connection

def suite():
	"""Generate test suite for this module"""
	res = helpers.NamedTestSuite()

	parameters = [
#		('disk.sqlite3', 'test.db', 0, 'sqlite3')
		('memory.sqlite3', None, None, 'sqlite3')
	]

	modules = [delete, insert, modify, read, search, connection]

	for parameter_list in parameters:
		for module in modules:
			res.addTest(unittest.TestLoader().loadTestsFromTestCase(
				requests.getParameterized(module.get_class(), *parameter_list)))

	return res
