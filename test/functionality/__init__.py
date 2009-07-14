def getParameterized(base_class, name_prefix, connection_generator,
	engine_tag, path, open_existing):
	"""Get named test suite with predefined setUp()"""
	class Derived(base_class):
		def setUp(self):
			self.db = path
			self.gen = connection_generator
			self.conn = self.gen.connect(path, open_existing, engine_tag)

		def tearDown(self):
			self.conn.close()

	Derived.__name__ = name_prefix

	return Derived
