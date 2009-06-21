import yaml
import functools

SIMPLE_TYPES = [
	int,
	str,
	float,
	bytes
]

def flattenHierarchy(data):
	def flattenNode(node, prefix=[]):
		if isinstance(node, dict):
			results = [flattenNode(node[x], list(prefix) + [x]) for x in node.keys()]
			return functools.reduce(list.__add__, results, [])
		elif isinstance(node, list):
			results = [flattenNode(x, list(prefix) + [i]) for i, x in enumerate(node)]
			return functools.reduce(list.__add__, results, [])
		elif node == None or node.__class__ in SIMPLE_TYPES:
			return [(prefix, node)]
		else:
			raise Exception("Unsupported type: " + node.__type__)

	return [(path, value) for path, value in flattenNode(data)]


class YamlParser:

	def parseRequest(self, request):
		data = yaml.load(request)

		print(repr(data['fields']))

		res = flattenHierarchy(data['fields'])
		for fld in res:
			print(fld)



y = YamlParser()
r = '''
type: add
id: 1
fields:
  name: Marty
  phone: 111
  friends:
    -
      name: Alice
      gender: female
      age: 22
      birthday:
    -
      name: Cat
      gender: male
      age: 4
'''
y.parseRequest(r)