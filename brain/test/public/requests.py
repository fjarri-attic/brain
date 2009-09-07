"""Unit with basic mechanisms of database layer requests testing"""

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
				{'Name': 'Track 1 name', 'Length': 'Track 1 length',
					'Authors': ['Alex', 'Bob']},
				{'Name': 'Track 2 name', 'Authors': ['Carl I']},
				{'Lyrics': ['Lalala']}
			]})

		self.id2 = self.conn.create({'tracks': [
				{'Name': 'Track 1 name', 'Length': 'Track 1 length',
					'Authors': ['Carl II', 'Dan']},
				{'Name': 'Track 2 name', 'Authors': ['Alex']},
				{'Name': 'Track 3 name', 'Authors': ['Rob']}
			]})

	def prepareStandDifferentTypes(self):
		"""Prepare DB with several objects and different value types"""
		self.id1 = self.conn.create({'name': 'Album 1',	'tracks': [
			{'Name': 'Track 1 name', 'Length': 300, 'Volume': 29.4,
				'Authors': ['Alex', 'Bob'],
				'Data': b'\x00\x01\x02'},
			{'Name': 'Track 2 name', 'Length': 350.0, 'Volume': 26,
				'Authors': ['Carl', 'Dan'],
				'Rating': 4,
				'Data': b'\x00\x01\x03'}],
			'meta': [
			 	'Pikeman', 'Archer', 1, 2, 4.0, 5.0, b'Gryphon', b'Swordsman'
			]})

		self.id2 = self.conn.create({'name': 'Album 2',	'tracks': [
			{'Name': 'Track 3 name', 'Length': 290, 'Volume': 33.0,
				'Authors': ['Earl', 'Fred', 'Greg'],
				'Data': b'\x00\x01\x04'},
			{'Name': 'Track 4 name', 'Length': 370, 'Volume': 22.1,
				'Authors': ['Hugo'],
				'Rating': 4,
				'Data': b'\x00\x01\x05'},
			{'Length': None, 'Volume': None, 'Rating': None}]})

		self.id3 = self.conn.create({'name': 'Album 3', 'tracks': [
				{'Length': None, 'Volume': 0, 'Rating': 0}
			]})

def getParameterized(base_class, engine_params, connection_generator):
	"""Get parameterized requests test class with predefined setUp()"""

	class Derived(base_class):
		def setUp(self):
			self.in_memory = engine_params.in_memory
			self.gen = connection_generator
			self.tag = engine_params.engine_tag

			args = engine_params.engine_args
			kwds = engine_params.engine_kwds

			self.connection_args = args
			self.connection_kwds = kwds
			self.conn = self.gen.connect(engine_params.engine_tag, *args, **kwds)

		def tearDown(self):
			self.conn.close()

	return Derived
