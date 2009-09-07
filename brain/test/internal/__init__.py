import helpers
from internal import engine, interface

def suite(db_path, all_engines, all_storages):
	internal_suite = helpers.NamedTestSuite('internal')
	internal_suite.addTest(interface.suite())
	internal_suite.addTest(engine.suite(db_path, all_engines, all_storages))
	return internal_suite
