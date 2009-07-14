"""Module which gathers all unittests"""

import optparse
import sys
import unittest

import internal.interface
import internal.engine
from functionality import delete, insert, modify, read, search, connection
import functionality
import helpers

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import brain

# Parse command line arguments

parser = optparse.OptionParser(usage = "run.py [options]")
parser.add_option("--ae", "--all-engines", action="store_true",
	dest="all_engines", help="run tests using all available DB engines")
parser.add_option("--ac", "--all-connections", action="store_true",
	dest="all_connections", help="run tests using all available Connection classes")
parser.add_option("--as", "--all-storages", action="store_true",
	dest="all_storages", help="run tests using all available storages")
parser.add_option("-v", "--verbosity", action="store", type="int", default=2,
	dest="verbosity", help="verbosity level, 0-3")

opts, args = parser.parse_args(sys.argv[1:])

# Prepare test suite

suite = helpers.NamedTestSuite()
suite.addTest(internal.interface.suite())

if opts.all_engines:
	engine_tags = brain.getEngineTags()
	engine_tags = {tag: tag for tag in engine_tags}
else:
	engine_tags = {'default': None}

if opts.all_storages:
	storages = {'memory': (None, None), 'file': ('test.db', 0)}
else:
	storages = {'memory': (None, None)}

connection_params = {}
for tag_str in engine_tags:
	for storage_str in storages:
		path, open_existing = storages[storage_str]
		test_tag = tag_str + "." + storage_str
		connection_params[test_tag] = (engine_tags[tag_str], path, open_existing)

# add engine class tests
for tag in connection_params:
	suite.addTest(internal.engine.suite(tag, *connection_params[tag]))

# add functionality tests

class XMLRPCGenerator:
	def __init__(self):
		self._client = brain.BrainClient('http://localhost:8000')

	def __getattr__(self, name):
		return getattr(self._client, name)

if opts.all_connections:
	connection_generators = {'local': brain, 'xmlrpc': XMLRPCGenerator()}
else:
	connection_generators = {'local': brain}

func_tests = {'delete': delete, 'insert': insert,
	'modify': modify, 'read': read, 'search': search, 'connection': connection}

for gen in connection_generators:
	for tag_str in engine_tags:
		for storage_str in storages:
			for func_test in func_tests:
				path, open_existing = storages[storage_str]
				test_tag = gen + "." + tag_str + "." + storage_str + "." + func_test
				connection_params[test_tag] = (engine_tags[tag_str], path, open_existing)
				suite.addTest(unittest.TestLoader().loadTestsFromTestCase(
					functionality.getParameterized(
					func_tests[func_test].get_class(),
					test_tag, connection_generators[gen],
					engine_tags[tag_str],
					path, open_existing
				)))

# Run tests

if opts.all_connections:
	xmlrpc_srv = brain.BrainServer()
	xmlrpc_srv.start()

helpers.TextTestRunner(verbosity=opts.verbosity).run(suite)

if opts.all_connections:
	xmlrpc_srv.stop()
