"""Unit-tests for database layer interface"""

import unittest

import testhelpers
from interfaces import *

class InterfaceTests(unittest.TestCase):
	"""Class which contains all interface testcases"""

	# Tests for Field class

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

	# Tests for common part of field-oriented requests

	def testRequestNoFields(self):
		"""Test that request can be created without fields"""
		r = ModifyRequest('1')

	def testRequestOneField(self):
		"""Test that request cannot be created if field is given as is"""
		self.failUnlessRaises(FormatError, ModifyRequest,
			'1', Field('test', 1))

	def testRequestListOfFields(self):
		"""Test that request can be created from list of fields"""
		r = ModifyRequest('1', [Field('test', 1), Field('test', 2)])

	def testRequestListOfNonFields(self):
		"""Test that request cannot be created if one of list elements is not Field"""
		self.failUnlessRaises(FormatError, ModifyRequest,
			'1', [Field('test', 1), "aaa"])

	def testRequestCopiesListOfFiels(self):
		"""Test that request constructor copies given list of fields"""
		f = Field('test', 2)
		l = [Field('test', 1), f]
		r = ModifyRequest('1', l)

		f.value = 3

		self.failUnlessEqual(r.fields[1].value, 2)

	# Additional checks for InsertRequest

	def testInsertRequestTargetIsNotField(self):
		"""Test that InsertRequest constructor fails if target is not Field"""
		self.failUnlessRaises(FormatError, InsertRequest,
			'1', "aaa", [Field('test', 1)])

	def testInsertRequestCopiesTarget(self):
		"""Test that InsertRequest constructor clones target field object"""
		f = Field(['test', 1], 2)
		r = InsertRequest('1', f, [Field('test', 1)])

		f.value = 3

		self.failUnlessEqual(r.target_field.value, 2)

	def testInsertRequestNotDeterminedTarget(self):
		"""Test that InsertRequest requires determined target"""
		self.failUnlessRaises(FormatError, InsertRequest,
			'1', Field(['test', None, 1]),
			[Field('test', 1)])

	def testInsertRequestTargetPointsToHash(self):
		"""Test that InsertRequest requires target pointing to list"""
		self.failUnlessRaises(FormatError, InsertRequest,
			'1', Field(['test', 1, 'aaa']),
			[Field('test', 1)])

def suite():
	"""Generate test suite for this module"""
	res = testhelpers.NamedTestSuite()
	res.addTest(unittest.TestLoader().loadTestsFromTestCase(InterfaceTests))
	return res

if __name__ == '__main__':
	testhelpers.TextTestRunner(verbosity=2).run(suite())
