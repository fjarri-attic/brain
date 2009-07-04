"""Module which gathers all unittests"""

import internal.interface
import internal.engine
import internal.parser
import functionality.requests_all
import helpers

def suite():
	res = helpers.NamedTestSuite()
	res.addTest(internal.interface.suite())
	res.addTest(internal.engine.suite())
	res.addTest(internal.parser.suite())

	res.addTest(functionality.requests_all.suite())
	return res

if __name__ == '__main__':
	helpers.TextTestRunner(verbosity=2).run(suite())
