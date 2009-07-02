"""Module which gathers all unittests"""

import test.functionality.requests_all
import test.functionality.interface
import test.functionality.engine
import test.functionality.parser
import test.helpers

def suite():
	res = test.helpers.NamedTestSuite()
	res.addTest(test.functionality.requests_all.suite())
	res.addTest(test.functionality.interface.suite())
	res.addTest(test.functionality.engine.suite())

	res.addTest(test.functionality.parser.suite())
	return res

if __name__ == '__main__':
	test.helpers.TextTestRunner(verbosity=2).run(suite())
