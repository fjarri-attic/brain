"""Module for running tests"""

import optparse
import sys

import doc
import func
import fuzz
import perf

# Parser settings

parser = optparse.OptionParser(usage = "run.py <mode> [options]\n" +
	"Modes: func, fuzz, doc, perf")

parser.add_option("--ae", "--all-engines", action="store_true",
	dest="all_engines", help="[func] run tests using all available DB engines")
parser.add_option("--ac", "--all-connections", action="store_true",
	dest="all_connections", help="[func] run tests using all available Connection classes")
parser.add_option("--as", "--all-storages", action="store_true",
	dest="all_storages", help="[func] run tests using all available storages")

parser.add_option("-o", "--objects", action="store", type="int", default=1,
	dest="objects", help="[fuzz] number of objects")
parser.add_option("-a", "--actions", action="store", type="int", default=100,
	dest="actions", help="[fuzz] number of actions")
parser.add_option("-s", "--seed", action="store", type="int", default=0,
	dest="seed", help="[fuzz] starting seed for random number generator")

parser.add_option("-v", "--verbosity", action="store", type="int", default=2,
	dest="verbosity", help="verbosity level, 0-3")

# Parse options and run tests

if len(sys.argv) == 1:
	parser.error("Error: mode should be specified")

# FIXME: find a way to do it using OptionParser
modes = ['func', 'fuzz', 'doc', 'perf']
mode = sys.argv[1]
args = sys.argv[2:]

if mode not in modes:
	parser.print_help()
	sys.exit(1)

opts, args = parser.parse_args(args)

if mode == 'func':
	func.runFunctionalityTests(all_connections=opts.all_connections,
		all_engines=opts.all_engines, all_storages=opts.all_storages,
		verbosity=opts.verbosity)
elif mode == 'fuzz':
	fuzz.runFuzzTest(objects=opts.objects, actions=opts.actions,
		seed=opts.seed, verbosity=opts.verbosity)
elif mode == 'doc':
	doc.runDocTest(verbosity=opts.verbosity)
elif mode == 'perf':
	perf.runPerformanceTests(verbosity=opts.verbosity)
