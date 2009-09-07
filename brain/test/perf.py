"""Functionality tests"""

import tempfile

import helpers
import public
import fuzz

import brain


def runPerformanceTests(verbosity=2):
	"""Start functionality tests suite"""

	print("Performance tests:")

	# folder for DBs which are represented by files
	db_path = tempfile.mkdtemp(prefix='braindb')

	test_suites = public.getEngineTestSuites(db_path)

	for test_suite in test_suites:
		test_time = helpers.TextTestRunner(verbosity=verbosity,
			show_report=False).run(test_suite)
		print("* Functionality tests: {0:.3f} s".format(test_time))

	total_times = None
	seeds = [100, 200, 300]
	for seed in seeds:
		if verbosity > 2:
			print("Running fuzz test for seed " + str(seed))
		times = fuzz.runFuzzTest(objects=5, seed=seed, show_report=False)
		if total_times is None:
			total_times = times
		else:
			for action in times:
				total_times[action] += times[action]

	time_strings = ["- " + action + ": {0:.3f} s".format(total_times[action])
		for action in total_times]
	print("* Fuzz test, seeds " + ", ".join([str(seed) for seed in seeds]) +
		", action times:\n" + "\n".join(time_strings))
