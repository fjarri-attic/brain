"""Module which gathers all unittests"""

import test_database
import testhelpers

def suite():
	res = testhelpers.NamedTestSuite()
	res.addTest(test_database.suite())
	return res	

if __name__ == '__main__':
	testhelpers.TextTestRunner(verbosity=2).run(suite())
