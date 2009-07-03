"""Unit tests for database layer modify request"""

import unittest

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import brain
from test import helpers
from test.functionality.requests import TestRequest

class Modify(TestRequest):
	"""Test different uses of ModifyRequest"""

	def testBlankObjectAddition(self):
		"""Check that object without fields cannot be created"""

		# Value is the mandatory paramter to this function
		self.failUnlessRaises(TypeError, self.conn.create)

		self.failUnlessRaises(brain.FacadeError, self.conn.create, {})
		self.failUnlessRaises(brain.FacadeError, self.conn.create, [])
		self.failUnlessRaises(brain.FacadeError, self.conn.create, None)

	def testModifyNothing(self):
		"""Check that modification without parameters does nothing"""
		orig_data = {'fld': 1}
		obj = self.conn.create(orig_data)
		self.conn.modify(obj, None)
		data = self.conn.read(obj)
		self.failUnlessEqual(data, orig_data)


def get_class():
	return Modify
