"""Functionality tests"""

import tempfile

import helpers
import internal
import public

import brain


def runFunctionalityTests(all_engines=False, all_connections=False, all_storages=False,
	verbosity=2, show_report=True):
	"""Start functionality tests suite"""

	if show_report:
		print("Functionality tests")

	# folder for DBs which are represented by files
	db_path = tempfile.mkdtemp(prefix='braindb')

	suite = helpers.NamedTestSuite()

	suite.addTest(internal.suite(db_path, all_engines, all_storages))
	suite.addTest(public.suite(db_path, all_engines, all_storages, all_connections,
		'http://localhost:8000'))

	# Run tests

	if all_connections:
		xmlrpc_srv = brain.Server(db_path=db_path)
		xmlrpc_srv.start()

	test_time = helpers.TextTestRunner(verbosity=verbosity, show_report=show_report).run(suite)

	if all_connections:
		xmlrpc_srv.stop()

	return test_time
