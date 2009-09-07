import brain

import helpers
from public import delete, insert, modify, read, search, connection
from internal import engine

def suite(db_path, all_engines, all_storages, all_connections):
	
	class XMLRPCGenerator:
		def __init__(self):
			self._client = brain.Client('http://localhost:8000')

		def __getattr__(self, name):
			return getattr(self._client, name)

	if all_connections:
		connection_generators = {'local': brain, 'xmlrpc': XMLRPCGenerator()}
	else:
		connection_generators = {'local': brain}

	res = helpers.NamedTestSuite('public')

	for gen in connection_generators:
		for engine_params in engine.getEngineTestParams(db_path, all_engines, all_storages):
			requests_suite = helpers.NamedTestSuite(gen + '.' + engine_params.test_tag)
			for module in [delete, insert, modify, read, search, connection]:
				requests_suite.addTest(module.suite(engine_params, 
					connection_generators[gen]))
			res.addTest(requests_suite)

	return res
