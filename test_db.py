"""Module which gathers all database unittests"""

import test_db_requests
import test_db_interface
import testhelpers

def suite():
	res = testhelpers.NamedTestSuite()
	res.addTest(test_db_requests.suite())
	res.addTest(test_db_interface.suite())
	return res

if __name__ == '__main__':
	testhelpers.TextTestRunner(verbosity=2).run(suite())
