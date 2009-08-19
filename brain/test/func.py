"""Functionality tests"""

import unittest
import sys
import tempfile
import os

import brain.test.helpers as helpers
import brain.test.internal.interface as interface
import brain.test.internal.engine as engine
import brain.test.public as public
from public import delete, insert, modify, read, search, connection

import brain

def runFunctionalityTests(all_engines=False, all_connections=False, all_storages=False, verbosity=2):
	"""Start functionality tests suite"""

	print("Functionality tests")

	IN_MEMORY = 'memory' # tag for in-memory DB tests

	suite = helpers.NamedTestSuite()
	suite.addTest(interface.suite())

	if all_engines:
		engine_tags = brain.getEngineTags()
		engine_tags = {tag: tag for tag in engine_tags}
	else:
		engine_tags = {brain.getDefaultEngineTag(): None}

	# folder for DBs which are represented by files
	db_path = tempfile.mkdtemp(prefix='braindb')

	storages = {
		'sqlite3': [(IN_MEMORY, (None,), {}), ('file', ('test.db',),
			{'open_existing': 0, 'db_path': db_path})],
		'postgre': [('tempdb', ('tempdb',), {'open_existing': 0,
			'port': 5432, 'user': 'postgres', 'password': ''})]
	}

	if not all_storages:
		# leave only default storages
		storages = {x: [storages[x][0]] for x in storages}

	# add engine class tests
	for tag_str in engine_tags:
		for storage in storages[tag_str]:
			storage_str, args, kwds = storage
			test_tag = tag_str + "." + storage_str
			suite.addTest(engine.suite(test_tag,
				engine_tags[tag_str], *args, **kwds))

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
		for tag_str in engine_tags:
			for storage in storages[tag_str]:
				for func_test in func_tests:
					storage_str, args, kwds = storage
					test_tag = gen + "." + tag_str + "." + storage_str + "." + func_test
					suite.addTest(unittest.TestLoader().loadTestsFromTestCase(
						public.getParameterized(
						func_tests[func_test].get_class(),
						test_tag, connection_generators[gen],
						engine_tags[tag_str],
						(storage_str == IN_MEMORY),
						*args, **kwds)))

	# Run tests

	if all_connections:
		xmlrpc_srv = brain.Server(db_path=db_path)
		xmlrpc_srv.start()

	helpers.TextTestRunner(verbosity=verbosity).run(suite)

	if all_connections:
		xmlrpc_srv.stop()
