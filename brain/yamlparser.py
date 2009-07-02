import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

from . import interface
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
		elif node is None or node.__class__ in SIMPLE_TYPES:
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
  tree:
    path:
    data:
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
  field:
    path:
      - friends
      - 2
      - name
    value: dog
'''

r2 = """
data:
  -
    path:
      - friends
      - 2
    value:
      name: Alice
      gender: F
      age: 22
      birthday:
      tags:
        - red-headed
        - glasses
        - reader
  -
    path:
      - friends
      - 1
      - age
    value: 23
  -
    - friends
    -
    - gender
  -
    path:
    value:
      name: Alice
      gender: F
      age: 22
      birthday:
"""

y.parseRequest(r)

def parseData(root):
	if isinstance(root, list):
		return interface.Field(root)
	else:
		value = root['value']
		if isinstance(value, list) or isinstance(value, dict):
			path = root['path']
			if path is None: path = []

			return (interface.Field(path),
				[interface.Field(path, val) for path, val in flattenHierarchy(value)])
		else:
			return interface.Field(root['path'], value)

r2_data = yaml.load(r2)
for elem in r2_data['data']:
	print("-" * 40)
	print(repr(parseData(elem)))
