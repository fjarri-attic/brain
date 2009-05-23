import interfaces
import yaml
import functools

SEPARATOR = "."
SIMPLE_TYPES = [
	int,
	str,
	float
]

def flattenHierarchy(data):
	def flattenNode(node, prefix=[]):
		if isinstance(node, dict):
			results = [flattenNode(node[x], list(prefix) + [x]) for x in node.keys()]
			return functools.reduce(list.__add__, results, [])
		elif isinstance(node, list):
			results = [flattenNode(x, list(prefix) + ['list']) for x in node]
			return functools.reduce(list.__add__, results, [])
		elif len(list(filter(lambda x: isinstance(node, x), SIMPLE_TYPES))) > 0:
			return [(prefix, node)]
		else:
			raise Exception("Unsupported type: " + node.__type__)

	return [(SEPARATOR.join(path), value) for path, value in flattenNode(data)]


class YamlParser(interfaces.RequestParser):

	def parseRequest(self, request):
		data = yaml.load(request)

		print(repr(data))

		print(flattenHierarchy(data['fields']))



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
    -
      name: Cat
      gender: male
'''
y.parseRequest(r)