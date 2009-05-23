"""Module which gathers all unittests"""

import test.db.requests_all
import test.db.interface
import test.helpers

def suite():
	res = test.helpers.NamedTestSuite()
	res.addTest(test.db.requests_all.suite())
	res.addTest(test.db.interface.suite())
	return res

if __name__ == '__main__':
	test.helpers.TextTestRunner(verbosity=2).run(suite())
