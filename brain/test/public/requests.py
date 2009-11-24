"""Unit with basic mechanisms of database layer requests testing"""

import copy
import unittest

import helpers

class TestRequest(helpers.NamedTestCase):
	"""Base class for database requests testing"""

	def prepareStandNoList(self):
		"""Prepare DB wiht several objects which contain only hashes"""
		self.id1 = self.conn.create({'name': 'Alex', 'phone': '1111'})
		self.id2 = self.conn.create({'name': 'Bob', 'phone': '2222'})
		self.id3 = self.conn.create({'name': 'Carl', 'phone': '3333', 'age': '27'})
		self.id4 = self.conn.create({'name': 'Don', 'phone': '4444', 'age': '20'})
		self.id5 = self.conn.create({'name': 'Alex', 'phone': '1111', 'age': '22'})

	def prepareStandSimpleList(self):
		"""Prepare DB with several objects which contain simple lists"""
		self.id1 = self.conn.create({'tracks': ['Track 1', 'Track 2', 'Track 3']})
		self.id2 = self.conn.create({'tracks': ['Track 2', 'Track 1']})

	def prepareStandNestedList(self):
		"""Prepare DB with several objects which contain nested lists"""
		self.id1 = self.conn.create({'tracks': [
				{'name': 'Track 1 name', 'length': 'Track 1 length',
					'authors': ['Alex', 'Bob']},
				{'name': 'Track 2 name', 'authors': ['Carl I']},
				{'lyrics': ['Lalala']}
			]})

		self.id2 = self.conn.create({'tracks': [
				{'name': 'Track 1 name', 'length': 'Track 1 length',
					'authors': ['Carl II', 'Dan']},
				{'name': 'Track 2 name', 'authors': ['Alex']},
				{'name': 'Track 3 name', 'authors': ['Rob']}
			]})

	def prepareStandDifferentTypes(self):
		"""Prepare DB with several objects and different value types"""
		self.id1 = self.conn.create({'name': 'Album 1', 'tracks': [
			{'name': 'Track 1 name', 'length': 300, 'volume': 29.4,
				'authors': ['Alex', 'Bob'],
				'data': b'\x00\x01\x02'},
			{'name': 'Track 2 name', 'length': 350.0, 'volume': 26,
				'authors': ['Carl', 'Dan'],
				'rating': 4,
				'data': b'\x00\x01\x03'}],
			'meta': [
			 	'Pikeman', 'Archer', 1, 2, 4.0, 5.0, b'Gryphon', b'Swordsman'
			]})

		self.id2 = self.conn.create({'name': 'Album 2', 'tracks': [
			{'name': 'Track 3 name', 'length': 290, 'volume': 33.0,
				'authors': ['Earl', 'Fred', 'Greg'],
				'data': b'\x00\x01\x04'},
			{'name': 'Track 4 name', 'length': 370, 'volume': 22.1,
				'authors': ['Hugo'],
				'rating': 4,
				'data': b'\x00\x01\x05'},
			{'length': None, 'volume': None, 'rating': None}]})

		self.id3 = self.conn.create({'name': 'Album 3', 'tracks': [
				{'length': None, 'volume': 0, 'rating': 0}
			]})

def getParameterized(base_class, engine_params, connection_generator):
	"""Get parameterized requests test class with predefined setUp()"""

	class Derived(base_class):

		def reconnect(self, **additional_kwds):
			"""Creates another connection with the same properties as the initial one"""
			args = self._connection_args
			kwds = copy.deepcopy(self._connection_kwds)
			kwds.update(additional_kwds)
			kwds['open_existing'] = 1
			return self.gen.connect(self._tag, *args, **kwds)

		def setUp(self):
			self.in_memory = engine_params.in_memory
			self.gen = connection_generator
			self._tag = engine_params.engine_tag

			args = engine_params.engine_args
			kwds = engine_params.engine_kwds

			self._connection_args = args
			self._connection_kwds = kwds
			self.conn = self.gen.connect(engine_params.engine_tag, *args, **kwds)

		def tearDown(self):
			self.conn.close()

	return Derived
