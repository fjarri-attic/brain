import brain

import helpers
from public import delete, insert, modify, read, search, connection
from internal import engine


class XMLRPCGenerator:
	"""Class which mimics brain interface"""

	def __init__(self, server_address):
		self._client = brain.Client(server_address)

	def __getattr__(self, name):
		return getattr(self._client, name)


def suite(db_path, all_engines=False, all_storages=False,
	all_connections=False, server_address=None):

	res = helpers.NamedTestSuite('public')

	test_suites = getEngineTestSuites(db_path, all_engines, all_storages,
		all_connections, server_address)

	for test_suite in test_suites:
		res.addTest(test_suite)

	return res

def getEngineTestSuites(db_path, all_engines=False, all_storages=False,
	all_connections=False, server_address=None):

	res = []

	if all_connections:
		connection_generators = {'local': brain,
			'xmlrpc': XMLRPCGenerator(server_address)}
	else:
		connection_generators = {'local': brain}

	for gen in connection_generators:
		for engine_params in engine.getEngineTestParams(db_path, all_engines, all_storages):
			requests_suite = helpers.NamedTestSuite(gen + '.' + engine_params.test_tag)
			for module in [delete, insert, modify, read, search, connection]:
				requests_suite.addTest(module.suite(engine_params,
					connection_generators[gen]))
			res.append(requests_suite)

	return res
