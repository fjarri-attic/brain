import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

from db import interface
import xml.etree.ElementTree
import functools

class ElemTreeParser:

	def parseRequest(self, request):

		handlers = {
			'add': self.__parseAddRequest
		}

		root = xml.etree.ElementTree.fromstring(request)

		# TODO: perform validation against DTD here

		#if not root.tag in handlers.keys():
		#	raise Exception("Unknown request type: " + root.tag)

		# Find id
		return self.__elementTreeToData(root)
		#target_id = root.find('id').text
		
		#return handlers[root.tag](target_id, root.find('fields'))

	def __elementToValue(self, elem):
		if elem != None:
			value = elem.text.strip()
			if value == '':
				value = None
			return value
		else:
			return None

	def __elementTreeToData(self, elem):
		name = elem.get('name')
		data_elem = elem.find('data')
		if data_elem != None:
			value = self.__elementToValue(data_elem)
			elem.remove(data_elem)
		else:
                        value = self.__elementToValue(elem)

		if elem.tag == 'field':
			return (name, value, None)
		elif elem.tag == 'fields':
			children_list = [self.__elementTreeToData(x) for x in elem.getchildren()]
			native_hash = { name: (value, child) for name, value, child in children_list }
			return (name, value, native_hash)
		elif elem.tag == 'list':
			list_elems = elem.getchildren()
			native_list = [self.__elementTreeToData(x) for x in list_elems]
			return (name, value, native_list)

	def __parseAddRequest(self, target_id, fields):
		t1, value, data = self.__elementTreeToData(fields)
		return value, data
		# return interface.RewriteRequest(target_id, value)


p = ElemTreeParser()
r = '''
<request>
	<type>add</type>
	<id>1</id>
	<fields><map>
		<field name='name' type='str'>Marty</field>
		<field name='phone' type='str'>111</field>
		<list name='friends'>
			<map>
				<field name='name' type='str'>Sasha</field>
				<field name='phone' type='int'>222</field>
			</map>
			<map>
				<field name='name' type='str'>Pasha</field>
				<field name='phone' type='int'>333</field>
			</map>
		</list>
		<map name='info'>
			<list name='friends'>
				<map>
					<field name='name' type='str'>Sasha</field>
					<field name='phone' type='int'>222</field>
				</map>
				<map>
					<field name='name' type='str'>Pasha</field>
					<field name='phone' type='int'>333</field>
				</map>
			</list>
			<field name='age' type='int'>20</field>
			<field name='height' type='int'>180</field>
		</map>
	</map></fields>
</request>
'''

def xmlToPython(data, separator):
	def flattenNode(node, prefix=[]):
		value, children = node
		if value != None:
			self_value = [(prefix, node[0])]
		else:
			self_value = []

		results = []
		if children != None:
			if isinstance(children, dict):
				lists = [flattenNode(children[x], list(prefix) + [x]) for x in children.keys()]
				results = functools.reduce(list.__add__, lists, [])
			elif isinstance(children, list):
				for i in range(len(children)):
					t, value, node = children[i]
					results += flattenNode((value, node), list(prefix) + [str(i)])

		return self_value + results

	return [(separator.join(path), value) for path, value in flattenNode(data)]

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

res = p.parseRequest(r)

print(res)
#l = flattenHierarchy((value, data), '.')
#	print(repr(path) + ": " + repr(value))

