"""Database fuzz testing"""

import random
import string
import copy
import traceback
import time
import sys

import brain
from brain.data import *

STARTING_DEPTH = 5 # starting data depth of objects
MAX_DEPTH = 5 # maximum depth of data structures created during test
MAX_ELEMENTS_NUM = 3 # maximum number of elements in created lists/dictionaries
STOP_DATA_GENERATION = 0.6 # probability of stopping data generation on each level of structure
STOP_PATH_GENERATION = 0.1 # probability of stopping path generation on each level of structure
NONE_PROBABILITY = 0.3 # probability of last None appearance in the path of insert request
READ_BY_MASK_PROBABILITY = 0.6 # probability of using masks in read()
CONFLICTING_PATH_PROBABILITY = 0.2 # probability of using conflicting path when modifying/inserting

class FakeConnection:
	"""
	Class which mimics some Connection methods, implementing them using Python data structures.
	It is used to test request results for real Connection class
	"""

	def __init__(self):
		self._id_counter = 0
		self._root = {}

	def _deleteAll(self, obj, path):
		"""Delete all values from given path"""
		if path[0] is None and isinstance(obj, list):
			if len(path) == 1:
				obj[:] = []
			else:
				for i in range(len(obj)):
					self._deleteAll(obj[i], path[1:])
		elif (isinstance(path[0], str) and isinstance(obj, dict) and path[0] in obj.keys()) or \
				(isinstance(path[0], int) and isinstance(obj, list) and path[0] < len(obj)):
			if len(path) == 1:
				del obj[path[0]]
			else:
				self._deleteAll(obj[path[0]], path[1:])

	def create(self, data, path=None):
		if path is None:
			path = []
		self._id_counter += 1
		saveToTree(self._root, self._id_counter, path, data)
		return self._id_counter

	def modify(self, id, path, value, remove_conflicts=False):
		saveToTree(self._root, id, path, value, remove_conflicts=remove_conflicts)

	def insertMany(self, id, path, values, remove_conflicts=False):
		if remove_conflicts:
			self.modify(id, path[:-1], [], remove_conflicts=True)

		target = getNodeByPath(self._root[id], path[:-1])
		index = path[-1]

		if index is not None and index > len(target):
			for i in range(len(target), index):
				target.append(None)

		if index is None:
			for value in values:
				target.append(value)
		else:
			for value in reversed(values):
				target.insert(index, value)

	def deleteMany(self, id, paths=None):
		for path in paths:
			self._deleteAll(self._root[id], path)

	def read(self, id, path=None, masks=None):
		if path is None:
			path = []

		res = getNodeByPath(self._root, [id] + path)

		if masks is not None:
			fields = treeToPaths(res)
			res_fields = []
			for field_path, value in fields:
				for mask in masks:
					if pathMatchesMask(field_path, mask):
						res_fields.append((field_path, value))
						break

			if len(res_fields) == 0:
				raise brain.LogicError("Object " + str(id) +
					" does not have fields matching given masks")

			res = pathsToTree(res_fields)

		return res

# Auxiliary functions

def getRandomString():
	return "".join(random.sample(string.ascii_letters + string.digits, 8))

def getRandomValue():
	funcs = [
		lambda: None,
		lambda: random.randint(-100, 100),
		lambda: random.random() * 200 - 100,
		lambda: getRandomString(),
		lambda: bytes(getRandomString(), 'ascii')
	]

	return random.choice(funcs)()

def getRandomData(depth):
	if depth == 0 or random.random() < STOP_DATA_GENERATION:
		return getRandomValue()

	length = random.randint(1, MAX_ELEMENTS_NUM)
	if random.choice([False, True]):
		return [getRandomData(depth - 1) for x in range(length)]
	else:
		return {getRandomString(): getRandomData(depth - 1) for x in range(length)}

def getRandomNonTrivialData(depth):
	"""Returns non-None random data structure"""
	data = None
	while not isinstance(data, list) and not isinstance(data, dict):
		data = getRandomData(depth)
	return data

def getRandomPath(root):
	"""Returns path to random element of given structure"""
	if random.random() < STOP_PATH_GENERATION:
		return []

	if isinstance(root, list):
		if len(root) == 0:
			return [None]
		else:
			random_index = random.randrange(len(root))
			return [random_index] + getRandomPath(root[random_index])
	elif isinstance(root, dict) and len(root) > 0:
		random_key = random.choice(list(root.keys()))
		return [random_key] + getRandomPath(root[random_key])
	else:
		return []

def getRandomDefinitePath(root):
	"""Returns random path without Nones"""
	path = [None]
	while None in path:
		path = getRandomPath(root)
	return path

def getRandomConflictingPath(root):
	path = []
	while len(path) == 0:
		path = getRandomDefinitePath(root)

	i = random.randint(0, len(path) - 1)
	if isinstance(path[i], str):
		path[i] = 0
	else:
		path[i] = getRandomString()
	return path

def getRandomDeletePath(root):
	"""
	Returns random path for delete request -
	definite non-zero path, because mask deletion is hard to support in fuzz test,
	and we want to avoid deletion of the object
	"""
	path = []
	while len(path) == 0:
		path = getRandomMask(root)
	return path

def getRandomInsertPath(root):
	"""
	Returns random path for the target of insert request -
	random path, pointing to list, probably with the None in the end
	Warning: the function will last forever if there is not any list in data
	"""
	path = []
	while len(path) == 0 or isinstance(path[-1], str):
		path = getRandomPath(root)
	if random.random() < NONE_PROBABILITY:
		path[-1] = None

	return path

def getRandomMask(root):
	path = getRandomPath(root)
	for i, e in enumerate(path):
		if not isinstance(e, str) and random.random() < NONE_PROBABILITY:
			path[i] = None
	return path

def listInData(data):
	"""Returns True if there is a list in given data structure"""
	if isinstance(data, list):
		return True
	elif isinstance(data, dict):
		for key in data:
			if listInData(data[key]): return True
		return False
	else:
		return False

def hasNonTrivialDefinitePath(data):
	if isinstance(data, list) and len(data) > 0:
		return True
	elif isinstance(data, dict) and len(data.keys()) > 0:
		return True

	return False


class RandomAction:
	"""Class, representing an action on a given object"""

	def __init__(self, obj_contents):
		self._obj_contents = obj_contents

		args_constructors = {
			'modify': self._constructModifyArgs,
			'insertMany': self._constructInsertArgs,
			'deleteMany': self._constructDeleteArgs,
			'read': self._constructReadArgs
		}

		if type(self._obj_contents) not in [list, dict] or len(self._obj_contents) == 0:
			del args_constructors['deleteMany']
		if not listInData(self._obj_contents):
			del args_constructors['insertMany']

		self._args = ()
		self._kwds = {}
		self._method = random.choice(list(args_constructors.keys()))
		args_constructors[self._method]()

	def dump(self, verbosity):
		return (str(self) if verbosity > 3 else self.getMethod())

	def _constructModifyArgs(self):
		data = getRandomData(MAX_DEPTH)
		if random.random() < CONFLICTING_PATH_PROBABILITY and \
				hasNonTrivialDefinitePath(self._obj_contents):
			path = getRandomConflictingPath(self._obj_contents)
			remove_conflicts = True
		else:
			path = getRandomDefinitePath(self._obj_contents)
			remove_conflicts = False

		self._args = (path, data, remove_conflicts)

	def _constructInsertArgs(self):
		elems = random.randint(1, MAX_ELEMENTS_NUM)
		path = getRandomInsertPath(self._obj_contents)

		if random.random() < CONFLICTING_PATH_PROBABILITY and len(path) > 1:
			i = random.randint(0, len(path) - 2)
			if isinstance(path[i], str):
				path[i] = 0
			else:
				path[i] = getRandomString()
			remove_conflicts = True
		else:
			remove_conflicts = False

		to_insert = [getRandomData(MAX_DEPTH) for i in range(elems)]
		self._args = (path, to_insert, remove_conflicts)

	def _constructDeleteArgs(self):
		self._args = ([getRandomDeletePath(self._obj_contents)],)

	def _constructReadArgs(self):
		path = getRandomDefinitePath(self._obj_contents)
		if random.random() < READ_BY_MASK_PROBABILITY:
			elems = random.randint(1, MAX_ELEMENTS_NUM)
			masks = [getRandomMask(getNodeByPath(self._obj_contents, path)) for i in range(elems)]
		else:
			masks = None

		self._args = (path, masks)

	def getMethod(self):
		return self._method

	def __call__(self, conn, obj_id):
		args = self._args
		kwds = self._kwds
		return getattr(conn, self._method)(obj_id, *args, **kwds)

	def __str__(self):
		return self._method + repr(self._args)


def _runTests(objects, actions, verbosity):
	"""Main test function. Create several objects and perform random actions on them."""

	# using default engine, because we are testing only DB logic here
	engine_tag = brain.getDefaultEngineTag()

	# create brain connection and fake Python-powered connection
	conn = brain.connect(None, name=None)
	fake_conn = FakeConnection()

	objs = []
	fake_objs = []
	times = {}

	# create objects
	for i in range(objects):
		data = getRandomNonTrivialData(STARTING_DEPTH)

		try:
			objs.append(conn.create(data))
		except:
			print("Error creating object: " + str(data))
			raise

		if verbosity > 2:
			print("Object " + str(i) + " created" +
				(", initial state: " + repr(data) if verbosity > 3 else ""))

		fake_objs.append(fake_conn.create(data))

	# perform test
	for c in range(actions):
		for i in range(objects):

			# copy original state in case we delete object or some error occurs
			fake_state_before = copy.deepcopy(fake_conn.read(fake_objs[i]))

			# try to read the real object from the database
			try:
				state_before = conn.read(objs[i])
			except:
				print("Error reading object " + str(i) +
					(": " + str(fake_state_before) if verbosity > 3 else ""))
				if verbosity > 3:
					conn._engine.dump()
				raise

			# create random action and test it on fake object
			action = RandomAction(state_before)
			fake_exception = None

			# some fuzz actions can lead to exceptions
			try:
				fake_result = action(fake_conn, fake_objs[i])
			except brain.BrainError as e:
				fake_exception = e

			# if object gets deleted, return its state to original
			fake_state_after = fake_conn.read(fake_objs[i])
			if fake_state_after is None:
				fake_conn.modify(fake_objs[i], [], fake_state_before)
				continue

			# try to perform action on a real object
			try:
				starting_time = time.time()
				result = action(conn, objs[i])
				action_time = time.time() - starting_time

				method = action.getMethod()
				if method not in times.keys():
					times[method] = action_time
				else:
					times[method] += action_time

			except brain.BrainError as e:
				if not (fake_exception is not None and type(e) == type(fake_exception)):
					raise
			except:
				print("Error performing action on object " + str(i) +
					": " + action.dump(verbosity))
				if verbosity > 3:
					print("State before: " + str(fake_state_before))
					conn._engine.dump()
				raise

			if verbosity > 2:
				print("Object " + str(i) + ", " + action.dump(verbosity))

			# try to read resulting object state
			try:
				state_after = conn.read(objs[i])
			except:
				print("Error reading object " + str(i) +
					((": " + str(fake_state_after)) if verbosity > 3 else ""))
				print("After action: " + action.dump(verbosity))
				if verbosity > 3:
					print("On state: " + str(fake_state_before))
					conn._engine.dump()
				raise

			# compare resulting states of real and fake objects
			if state_after != fake_state_after:
				print("Action results are different:")
				print("State before: " + repr(fake_state_before))
				print("Action: " + action.dump(verbosity))
				print("Main state after: " + repr(state_after))
				print("Fake state after: " + repr(fake_state_after))
				if verbosity > 3:
					conn._engine.dump()
				raise Exception("Functionality error")

			# compare action return values (if any)
			if result != fake_result and fake_exception is None:
				print("Action return values are different:")
				print("State before: " + repr(fake_state_before))
				print("Action: " + action.dump(verbosity))
				print("Main return value: " + repr(result))
				print("Fake return value: " + repr(fake_result))
				if verbosity > 3:
					conn._engine.dump()
				raise Exception("Functionality error")

	return times

def runFuzzTest(objects=1, actions=100, verbosity=2, seed=0, show_report=True):
	"""Main test function. Start test, terminate on first exception"""

	if show_report:
		print("Fuzz test")

	if seed != 0:
		random.seed(seed)
		state = "seed: " + str(seed)
	else:
		random.seed()
		state = "random seed"

	if show_report:
		print(str(objects) + " objects, " + str(actions) + " actions, " + state)
		print("=" * 70)

	try:
		times = _runTests(objects, actions, verbosity)
	except:
		times = None
		err_class, err_obj, err_tb = sys.exc_info()
		print("Error during test: " + str(err_obj))

		# report traceback
		if verbosity > 1:
			traceback.print_tb(err_tb)
	finally:
		if show_report:
			print("=" * 70)

	if show_report and times is not None:
		time_strings = ["- " + action + ": {0:.3f} s".format(times[action]) for action in times]
		print("Action times:\n" + "\n".join(time_strings))

	return times
