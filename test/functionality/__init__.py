def getParameterized(base_class, name_prefix, connection_generator,
	engine_tag, in_memory, *args, **kwds):
	"""Get named test suite with predefined setUp()"""
	class Derived(base_class):
		def setUp(self):
			self.in_memory = in_memory
			self.gen = connection_generator
			self.tag = engine_tag
			self.connection_args = args
			self.connection_kwds = kwds
			self.conn = self.gen.connect(engine_tag, *args, **kwds)

		def tearDown(self):
			self.conn.close()

	Derived.__name__ = name_prefix

	return Derived
