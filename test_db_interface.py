"""Unit-tests for database layer interface"""

import unittest

import testhelpers
from interfaces import *

class InterfaceTests(unittest.TestCase):
	"""Class which contains all interface testcases"""

	def testFieldInitWithStr(self):
		"""Test field creation with string name"""
		f = Field('test', '1')
		self.failUnlessEqual(f.name, ['test'])

	def testFieldInitWithList(self):
		"""Test field creation with list name"""
		f = Field(['test', 1, None], '1')

	def testFieldInitWithHash(self):
		"""Test field creation with hash name"""
		self.failUnlessRaises(FormatError, Field, {'test': 1}, '1')

	def testFieldInitWithWrongList(self):
		"""Test field creation with wrong element in name list"""
		self.failUnlessRaises(FormatError, Field, ['test', 1, [1, 2]], '1')

	def testFieldInitWithEmptyName(self):
		"""Test field creation with empty string as a name"""
		self.failUnlessRaises(FormatError, Field, '', '1')

	def testFieldInitWithNoName(self):
		"""Test field creation with None as a name"""
		self.failUnlessRaises(FormatError, Field, None, '1')

	def testFieldCopiesList(self):
		"""Regression for bug when Field did not copy initializing list"""
		l = ['test', 1]
		f = Field(l, '1')
		l[1] = 2
		self.failUnlessEqual(f.name[1], 1)

	def testFieldEq(self):
		"""Test == operator for equal fields"""
		f1 = Field(['test', 1, None], 1)
		f2 = Field(['test', 1, None], 1)
		self.failUnlessEqual(f1, f2)

	def testFieldNonEqNames(self):
		"""Test == operator for fields with different names"""
		f1 = Field(['test1', 1, None], 1)
		f2 = Field(['test2', 2, None], 1)
		self.failIfEqual(f1, f2)

	def testFieldNonEqValues(self):
		"""Test == operator for fields with different values"""
		f1 = Field(['test1', 1, None], 1)
		f2 = Field(['test2', 1, None], 2)
		self.failIfEqual(f1, f2)

def suite():
	"""Generate test suite for this module"""
	res = testhelpers.NamedTestSuite()
	res.addTest(unittest.TestLoader().loadTestsFromTestCase(InterfaceTests))
	return res

if __name__ == '__main__':
	testhelpers.TextTestRunner(verbosity=2).run(suite())

