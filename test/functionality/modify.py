"""Unit tests for database layer modify request"""

import unittest

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

from test import helpers
from test.functionality.requests import TestRequest

class Modify(TestRequest):
	"""Test different uses of ModifyRequest"""

	def testBlankObjectAddition(self):
		"""Check that object without fields cannot be created"""
		self.failUnlessRaises(Exception, self.conn.create)

	def testModifyNothing(self):
		"""Check that modification without parameters does nothing"""
		obj = self.conn.create({'fld': 1})
		self.conn.modify(obj, None)


def get_class():
	return Modify
