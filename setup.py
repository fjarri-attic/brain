try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

VERSION = '0.0.11'

setup(
	name='brain',
	packages=['brain', 'brain.test', 'brain.test.public', 'brain.test.internal'],
	version=VERSION,
	author='Bogdan Opanchuk',
	author_email='bg@bk.ru',
	url='http://github.com/Manticore/brain',
	description='DDB front-end for SQL engines',
	classifiers=[
		'Development Status :: 3 - Alpha',
		'Environment :: Console',
		'Intended Audience :: Developers',
		'License :: OSI Approved :: GNU General Public License (GPL)',
		'Operating System :: OS Independent',
		'Programming Language :: Python :: 3',
		'Topic :: Database :: Front-Ends'
	]
)
