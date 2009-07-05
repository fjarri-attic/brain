"""Unit tests for database facade"""

import unittest

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import brain
from functionality.requests import TestRequest

class Connection(TestRequest):
	"""Test different uses of ModifyRequest"""

	def testAutocommitNoErrors(self):
		"""Check autocommit works when there are no exceptions raised"""

		# we cannot test autocommit on in-memory database
		if self.db == None: return

		obj = self.conn.create({'name': 'Alex', 'age': 22})

		# now open another connection to this database and check that
		# changes were really made
		conn2 = brain.connect(self.db)
		res = conn2.read(obj)
		self.assertEqual(res, {'name': 'Alex', 'age': 22})


def get_class():
	return Connection
