def getParameterized(base_class, name_prefix, connection_generator,
	engine_tag, name, open_existing):
	"""Get named test suite with predefined setUp()"""
	class Derived(base_class):
		def setUp(self):
			self.db = name
			self.gen = connection_generator
			self.tag = engine_tag
			self.conn = self.gen.connect(engine_tag, name=name,
				open_existing=open_existing)

		def tearDown(self):
			self.conn.close()

	Derived.__name__ = name_prefix

	return Derived
