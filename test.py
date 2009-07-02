"""Module which gathers all unittests"""

import test.internal.requests_all
import test.internal.interface
import test.internal.engine
import test.internal.parser
import test.helpers

def suite():
	res = test.helpers.NamedTestSuite()
	res.addTest(test.internal.requests_all.suite())
	res.addTest(test.internal.interface.suite())
	res.addTest(test.internal.engine.suite())

	res.addTest(test.internal.parser.suite())
	return res

if __name__ == '__main__':
	test.helpers.TextTestRunner(verbosity=2).run(suite())
