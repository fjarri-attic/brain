"""Functionality tests"""

import tempfile

import helpers
import public
import fuzz

import brain


def runPerformanceTests(all_engines=False, all_storages=False, verbosity=2):
	"""Start functionality tests suite"""

	print("Performance tests:")

	# folder for DBs which are represented by files
	db_path = tempfile.mkdtemp(prefix='braindb')

	test_suites = public.getEngineTestSuites(db_path, all_engines, all_storages)

	print("* Functionality tests")
	for test_suite in test_suites:
		test_time = helpers.TextTestRunner(verbosity=verbosity, 
			show_report=False).run(test_suite)
		print("- " + test_suite.getTag() + ": {0:.3f} s".format(test_time))

	for seed in [100, 200, 300]:
		times = fuzz.runFuzzTest(objects=5, seed=seed, show_report=False)
		time_strings = ["- " + action + ": {0:.3f} s".format(times[action]) 
			for action in times]
		print("* Fuzz test, seed " + str(seed) + 
			", action times:\n" + "\n".join(time_strings))
