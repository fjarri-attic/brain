"""
Module with helper functions for data structures manipulation
"""

import functools

def treeToPaths(node, prefix=[]):
	"""Transform list/dictionary to a list of (path, value) elements"""
	if isinstance(node, dict):
		results = [treeToPaths(node[x], prefix + [x]) for x in node.keys()]
		return functools.reduce(list.__add__, results, [(prefix, dict())])
	elif isinstance(node, list):
		results = [treeToPaths(x, prefix + [i]) for i, x in enumerate(node)]
		return functools.reduce(list.__add__, results, [(prefix, list())])
	else:
		return [(prefix, node)]

def saveToTree(obj, ptr, path, value, remove_conflicts=False):
	"""Save given value to a place in hierarchy, defined by pointer"""

	# ensure that there is a place in obj where ptr points
	if isinstance(obj, list) and len(obj) < ptr + 1:
		# extend the list to corresponding index
		obj.extend([None] * (ptr + 1 - len(obj)))
	elif isinstance(obj, dict) and ptr not in obj:
		# create dictionary key
		obj[ptr] = None

	if len(path) == 0:
	# if we are in leaf now, store value
		obj[ptr] = value
	else:
	# if not, create required structure and call this function recursively
		if obj[ptr] is None:
			obj[ptr] = {} if isinstance(path[0], str) else []
		else:
			if (isinstance(path[0], str) and not isinstance(obj[ptr], dict)) or \
					(not isinstance(path[0], str) and not isinstance(obj[ptr], list)):
				if remove_conflicts:
					obj[ptr] = {} if isinstance(path[0], str) else []
				else:
					raise Exception("Conflict encountered at " + str(path))

		saveToTree(obj[ptr], path[0], path[1:], value, remove_conflicts)

def pathsToTree(fields):
	"""Transform list of (path, value) tuples to nested dictionaries and lists"""

	if len(fields) == 0:
		return []

	# we need some starting object, whose pointer we can pass to recursive saveTo()
	res = []

	# sort fields before saving to dictionary
	# sort criterion: shorter paths go before longer paths
	# This sort gives us an ability to store {}s and []s without any checks,
	# because now they cannot rewrite already created dictionary or list
	# (because their elements will have longer paths)
	def key(field_tuple):
		path, value = field_tuple
		return len(path)

	for path, value in sorted(fields, key=key):
		saveToTree(res, 0, path, value)

	# get rid of temporary root object and return only its first element
	return res[0]

def getNodeByPath(obj, path):
	"""Get pointer to data structure with given path"""
	if len(path) == 0:
		return obj
	elif len(path) == 1:
		return obj[path[0]]
	else:
		return getNodeByPath(obj[path[0]], path[1:])

def pathMatchesMask(path, mask):
	if len(mask) > len(path):
		return False

	if len(mask) == 0:
		return True

	satisfies = True
	for i, e in enumerate(mask):
		if e != path[i] and not (e is None and isinstance(path[i], int)):
			return False

	return True


class AccessLogger:
	"""Class which remembers the order of access to some keys"""

	class Link:
		"""Auxiliary class, linked list element"""
		def __init__(self, prev, next, key):
			self.prev = prev
			self.next = next
			self.key = key

	def __init__(self, size_threshold):
		self._size_threshold = size_threshold

		self._map = {}

		# root element, linking the beginning and the end of the list
		self._root = self.Link(None, None, None)
		self._root.prev = self._root
		self._root.next = self._root

	def delete(self, key):
		"""Remove element from list"""
		old_link = self._map.pop(key)
		old_link.prev.next = old_link.next
		old_link.next.prev = old_link.prev

	def _push(self, key):
		"""Push element to the end of the list"""
		root = self._root
		last = root.prev
		link = self.Link(last, root, key)
		last.next = link
		root.prev = link
		self._map[key] = link

	def delete_oldest(self):
		"""
		Delete oldest elements (the ones at the beginning of the list,
		in other words - least recently updated) and return list with
		deleted elements.
		"""
		oldest_num = len(self._map) - self._size_threshold
		if oldest_num <= 0:
			return []

		oldest = []
		while len(oldest) < oldest_num:
			to_delete = self._root.next.key
			self.delete(to_delete)
			oldest.append(to_delete)

		return oldest

	def update(self, key):
		"""
		Move element to the end of the list or create it if
		it does not exist.
		"""
		if key in self._map:
			self.delete(key)

		self._push(key)
