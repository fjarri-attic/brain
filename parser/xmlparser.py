import interfaces
import xml.etree.ElementTree
import functools

class ElemTreeParser(interfaces.RequestParser):

	def parseRequest(self, request):

		handlers = {
			'add': self.__parseAddRequest
		}

		root = xml.etree.ElementTree.fromstring(request)

		# TODO: perform validation against DTD here
		
		if not root.tag in handlers.keys():
			raise Exception("Unknown request type: " + root.tag)
		
		## Find id
		target_id = root.find('id').text
		return handlers[root.tag](target_id, root.find('fields'))
	
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
		# return interfaces.RewriteRequest(target_id, value)
		

p = ElemTreeParser()
r = '''
<add>
	<id>1</id>
	<fields>
		<field name='name'>Marty</field>
		<field name='phone'>111</field>
		<list name='friends'>
			<data>Friends list</data>
			<fields>
				<field name='name'>Sasha</field>
				<field name='phone'>222</field>
			</fields>
			<fields>
				<field name='name'>Pasha</field>
				<field name='phone'>333</field>
			</fields>
		</list>
		<fields name='info'>
			<list name='friends'>
				<data>Friends list</data>
				<fields>
					<field name='name'>Sasha</field>
					<field name='phone'>222</field>
				</fields>
				<fields>
					<field name='name'>Pasha</field>
					<field name='phone'>333</field>
				</fields>
			</list>
			<field name='age'>20</field>
			<field name='height'>180</field>
		</fields>
	</fields>
</add>
'''

def flattenHierarchy(data, separator):
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


value, data = p.parseRequest(r)

#print(repr(data))
l = flattenHierarchy((value, data), '.')

for path, value in l:
	print(repr(path) + ": " + repr(value))

