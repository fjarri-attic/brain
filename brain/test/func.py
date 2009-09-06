"""Functionality tests"""

import unittest
import sys
import tempfile
import os

import helpers
import internal.interface as interface
import internal.engine as engine
import public
from public import delete, insert, modify, read, search, connection

import brain


class EngineTestSuite(helpers.NamedTestSuite):

	def __init__(self, engine_tag, storage_tag, in_memory, engine_args, engine_kwds):
		test_tag = engine_tag + "." + storage_tag
		helpers.NamedTestSuite.__init__(self, test_tag)

		self.in_memory = in_memory
		self.engine_tag = engine_tag
		self.engine_args = engine_args
		self.engine_kwds = engine_kwds


def getEngineTestSuites(db_path, all_engines, all_storages):

	IN_MEMORY = 'memory' # tag for in-memory DB tests

	storages = {
		'sqlite3': [(IN_MEMORY, (None,), {}), ('file', ('test.db',),
			{'open_existing': 0, 'db_path': db_path})],
		'postgre': [('tempdb', ('tempdb',), {'open_existing': 0,
			'port': 5432, 'user': 'postgres', 'password': ''})]
	}

	if not all_storages:
		# leave only default storages
		storages = {x: [storages[x][0]] for x in storages}

	if not all_engines:
		default_tag = brain.getDefaultEngineTag()
		storages = {default_tag: storages[default_tag]}

	res = []

	for engine_tag in storages:
		for storage_tag, args, kwds in storages[engine_tag]:
			res.append(EngineTestSuite(engine_tag, storage_tag,
				(storage_tag == IN_MEMORY), args, kwds))

	return res

def runFunctionalityTests(all_engines=False, all_connections=False, all_storages=False,
	verbosity=2, show_report=True):
	"""Start functionality tests suite"""

	if show_report:
		print("Functionality tests")

	suite = helpers.NamedTestSuite()

	internal_suite = helpers.NamedTestSuite('internal')
	suite.addTest(internal_suite)

	public_suite = helpers.NamedTestSuite('public')
	suite.addTest(public_suite)

	internal_suite.addTest(interface.suite())

	# folder for DBs which are represented by files
	db_path = tempfile.mkdtemp(prefix='braindb')

	# add engine class tests
	for engine_suite in getEngineTestSuites(db_path, all_engines, all_storages):
		args = engine_suite.engine_args
		kwds = engine_suite.engine_kwds
		engine_suite.addTest(engine.suite(engine_suite.engine_tag, *args, **kwds))
		internal_suite.addTest(engine_suite)

	# add functionality tests

	class XMLRPCGenerator:
		def __init__(self):
			self._client = brain.Client('http://localhost:8000')

		def __getattr__(self, name):
			return getattr(self._client, name)

	if all_connections:
		connection_generators = {'local': brain, 'xmlrpc': XMLRPCGenerator()}
	else:
		connection_generators = {'local': brain}

	func_tests = {'delete': delete, 'insert': insert,
		'modify': modify, 'read': read, 'search': search, 'connection': connection}

	for gen in connection_generators:
		gen_suite = helpers.NamedTestSuite(gen)
		for engine_suite in getEngineTestSuites(db_path, all_engines, all_storages):
			for func_test in func_tests:
				args = engine_suite.engine_args
				kwds = engine_suite.engine_kwds
				func_suite = helpers.NamedTestSuite()
				func_suite.addTestCaseClass(public.getParameterized(
					func_tests[func_test].get_class(),
					func_test,
					connection_generators[gen],
					engine_suite.engine_tag,
					engine_suite.in_memory,
					*args, **kwds))
				engine_suite.addTest(func_suite)
			gen_suite.addTest(engine_suite)
		public_suite.addTest(gen_suite)


	# Run tests

	if all_connections:
		xmlrpc_srv = brain.Server(db_path=db_path)
		xmlrpc_srv.start()

	test_time = helpers.TextTestRunner(verbosity=verbosity, show_report=show_report).run(suite)

	if all_connections:
		xmlrpc_srv.stop()

	return test_time
