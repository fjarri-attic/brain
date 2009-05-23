"""Unit-tests for database layer interface"""

import unittest

import testhelpers
from interfaces import *

class InterfaceTests(unittest.TestCase):
	"""Class which contains all interface testcases"""

	def testTest(self):
		pass


def suite():
	res = testhelpers.NamedTestSuite()
	res.addTest(unittest.TestLoader().loadTestsFromTestCase(InterfaceTests))
	return res

if __name__ == '__main__':
	testhelpers.TextTestRunner(verbosity=2).run(suite())

