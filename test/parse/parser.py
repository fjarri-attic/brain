"""Unit tests for XML parser"""

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import unittest
from test import helpers
from parse.parser import *

class Parsing(unittest.TestCase):
	"""Test operation of XML parser"""

	def setUp(self):
		self.parser = ElemTreeParser()

	def testTemp(self):
		"""Check deletion of the whole object"""
		pass

def suite():
	res = helpers.NamedTestSuite()
	res.addTest(unittest.TestLoader().loadTestsFromTestCase(Parsing))
	return res
