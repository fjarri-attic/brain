"""Module which gathers all unittests"""

import test_db
import testhelpers

def suite():
	res = testhelpers.NamedTestSuite()
	res.addTest(test_db.suite())
	return res

if __name__ == '__main__':
	testhelpers.TextTestRunner(verbosity=2).run(suite())
