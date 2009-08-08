"""Module for running tests"""

import optparse
import sys

import functionality

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

functionality.runFunctionalityTests(all_connections=opts.all_connections, all_engines=opts.all_engines,
    all_storages=opts.all_storages, verbosity=opts.verbosity)
