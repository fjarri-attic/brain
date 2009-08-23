try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import os.path
import sys

VERSION = '0.1.0'

DOCUMENTATION = open(os.path.join(os.path.dirname(__file__), 'documentation.rst')).read()

setup(
	name='brain',
	packages=['brain', 'brain.test', 'brain.test.public', 'brain.test.internal'],
	data_files=[('docs', ['documentation.rst', 'changelog.rst'])],
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
