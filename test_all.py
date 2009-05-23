"""Module which gathers all unittests"""

import test_database
import test_db_interface
import testhelpers

def suite():
	res = testhelpers.NamedTestSuite()
	res.addTest(test_database.suite())
	res.addTest(test_db_interface.suite())
	return res

if __name__ == '__main__':
	testhelpers.TextTestRunner(verbosity=2).run(suite())
