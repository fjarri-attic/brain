"""Unit tests for database facade"""

import unittest

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

from test import helpers
from test.functionality.requests import TestRequest

class Facade(TestRequest):
	"""Test different uses of ModifyRequest"""

	def testStub(self):
		pass


def get_class():
	return Facade
