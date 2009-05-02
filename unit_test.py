class Testgroup:
	def __init__(self):
		self.nodes = []
		self.name = ""
		self.full_name = ""
		
	def registerTestcase(self, name, func):
		self.nodes += (self.name + name, node)
	
	def registerTestgroup(self, name, group):
		group.name = name
		group.full_name = self.name + "." + name
		self.nodes += group
	
	def run(self):
		for name, node in self.nodes:
			if isinstance(node, Testgroup):
				node.run()
			else:
				result = "PASS"
				try:
					node()
				except Exception as e:
					result = "FAIL\n" + str(e)
				print(self.full_name + "." + name + ": " + result)
