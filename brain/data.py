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

def saveToTree(obj, ptr, path, value):
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
		if (value != [] and value != {}) or obj[ptr] is None:
			obj[ptr] = value
	else:
	# if not, create required structure and call this function recursively
		if obj[ptr] is None:
			if isinstance(path[0], str):
				obj[ptr] = {}
			else:
				obj[ptr] = []

		saveToTree(obj[ptr], path[0], path[1:], value)

def pathsToTree(fields):
	"""Transform list of (path, value) tuples to nested dictionaries and lists"""

	if len(fields) == 0:
		return []

	# we need some starting object, whose pointer we can pass to recursive saveTo()
	res = []

	for path, value in fields:
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
