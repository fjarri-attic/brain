"""Custom unit-test module"""

import unittest
import sys
import traceback
import time

class _StreamWrapper:
	"""Used to decorate file-like objects with some handy methods"""

	def __init__(self, stream):
		self.__stream = stream

	def __getattr__(self, attr):
		"""Pass all unknown calls to stream"""
		return getattr(self.__stream,attr)

	def writeln(self, arg=None):
		"""Write line to stream"""
		if arg: self.write(arg)
		self.write('\n')


class _ResultCollector(unittest.TestResult):
	"""Extended result collector with convenient reporting"""

	def __init__(self, stream, verbosity):
		unittest.TestResult.__init__(self)
		self.__stream = stream

		# reporting options
		self.__show_fails = True if verbosity > 0 else False
		self.__show_passes = True if verbosity > 2 else False
		self.__show_traceback = True if verbosity > 1 else False

		# stack for tags of test suites
		self.__levels = []

	def enter(self, tag):
		"""Start named test suite"""
		self.__levels.append(tag)

	def leave(self):
		"""End of named test suite"""
		self.__levels.pop()

	def __reportTestcase(self, result, test, err=None):
		"""Write information about testcase to stream"""

		# report result
		full_tag = test.id()
		if len(self.__levels):
			full_tag = ".".join(self.__levels) + "." + full_tag
		self.__stream.writeln(result + " " + full_tag)

		if err:
			err_class, err_obj, err_tb = err

			# report error message
			self.__stream.writeln("! " + str(err_obj))

			# report traceback
			if self.__show_traceback:
				traceback.print_tb(err_tb, None, self.__stream)

	def addSuccess(self, test):
		"""Called on testcase success"""
		unittest.TestResult.addSuccess(self, test)
		if self.__show_passes:
			self.__reportTestcase("[pass]", test)

	def addFailure(self, test, err):
		"""Called on testcase failure"""
		unittest.TestResult.addFailure(self, test, err)
		if self.__show_fails:
			self.__reportTestcase("[FAIL]", test, err)

	def addError(self, test, err):
		"""Called on testcase error"""
		unittest.TestResult.addError(self, test, err)
		if self.__show_fails:
			self.__reportTestcase("[ERR] ", test, err)


class NamedTestSuite(unittest.TestSuite):
	"""Wrapper for standard TestSuite with naming support"""

	def __init__(self, tag=None):
		unittest.TestSuite.__init__(self)
		self.__tag = tag

	def addTestCaseClass(self, tc_class):
		"""Add all testcases from given class to suite"""
		test_loader = unittest.TestLoader()
		self.addTest(test_loader.loadTestsFromTestCase(tc_class))

	def run(self, result):
		"""Override for default TestSuite.run() method"""

		# pass tag to result collector
		if self.__tag is not None:
			result.enter(self.__tag)

		unittest.TestSuite.run(self, result)

		# tell result collector that test suite has finished
		if self.__tag is not None:
			result.leave()

	def getTag(self):
		return self.__tag


class NamedTestCase(unittest.TestCase):
	"""Overridden TestCase class which uses just test name as a tag"""

	def id(self):
		return self._testMethodName


class TextTestRunner:
	"""Simple test runner"""

	def __init__(self, stream=sys.stderr, verbosity=1, show_report=True):
		self.__stream = _StreamWrapper(stream)
		self.__verbosity = verbosity
		self.__show_report = show_report

	def run(self, suite):
		"""Run all tests from given suite"""

		res = _ResultCollector(self.__stream, self.__verbosity)

		if self.__show_report:
			self.__stream.writeln("=" * 70)
		time1 = time.time()
		suite.run(res)
		time2 = time.time()
		if self.__show_report:
			self.__stream.writeln("=" * 70)
			self.__stream.writeln("Finished in {0:.3f} seconds".format(time2 - time1))

			# Display results
			if not res.wasSuccessful():
				failures, errors = len(res.failures), len(res.errors)
				self.__stream.writeln("FAIL: {failures} failures, {errors} errors, {passed} passed"
					.format(failures=failures, errors=errors,
					passed=(res.testsRun - failures - errors)))
			else:
				self.__stream.writeln("OK: " + str(res.testsRun) + " testcases passed")

		return time2 - time1
