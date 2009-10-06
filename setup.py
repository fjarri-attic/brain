# Currently only py3k is supported
import sys
major, minor, micro, releaselevel, serial = sys.version_info
if major != 3:
	print("Python " + str(sys.version_info) + " is used; brain supports py3k only.")
	sys.exit(1)

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import os.path

from brain.test.doc import DOCUMENTATION

VERSION = '0.1.4'

# generate .rst file with documentation
open(os.path.join(os.path.dirname(__file__), 'documentation.rst'), 'w').write(DOCUMENTATION)

setup(
	name='brain',
	packages=['brain', 'brain.test', 'brain.test.public', 'brain.test.internal'],
	version=VERSION,
	author='Bogdan Opanchuk',
	author_email='bg@bk.ru',
	url='http://github.com/Manticore/brain',
	description='DDB front-end for SQL engines',
	long_description=DOCUMENTATION,
	classifiers=[
		'Development Status :: 4 - Beta',
		'Environment :: Console',
		'Intended Audience :: Developers',
		'License :: OSI Approved :: GNU General Public License (GPL)',
		'Operating System :: OS Independent',
		'Programming Language :: Python :: 3',
		'Topic :: Database :: Front-Ends'
	]
)
