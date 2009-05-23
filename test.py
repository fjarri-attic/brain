"""Module which gathers all unittests"""

import test.interface
import test.requests
import test.helpers

def suite():
	res = test.helpers.NamedTestSuite()
	res.addTest(test.requests.suite())
	res.addTest(test.interface.suite())
	return res

if __name__ == '__main__':
	test.helpers.TextTestRunner(verbosity=2).run(suite())
