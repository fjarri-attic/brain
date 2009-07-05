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

		data = {'name': 'Alex', 'age': 22}
		obj = self.conn.create(data)

		# now open another connection to this database and check that
		# changes were really made
		conn2 = brain.connect(self.db)
		res = conn2.read(obj)
		self.assertEqual(res, data)

	def testAutocommitRollbacksOnError(self):
		"""Check that in autocommit mode there will be rollback if action raises exception"""
		data = {'name': 'Alex', 'friends': ['Bob', 'Carl']}
		obj = self.conn.create(data)

		# this should raise a error, because we are trying
		# to create map where there is already a list
		self.assertRaises(brain.StructureError, self.conn.modify,
			obj, 2, ['friends', 'fld'])

		res = self.conn.read(obj)
		self.assertEqual(res, data)


def get_class():
	return Connection
