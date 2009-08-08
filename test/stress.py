import random
import string
import copy

import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

import brain
from brain.connection import FakeConnection

engine_tag = brain.getDefaultEngineTag()

conn = brain.connect(None, name=None)
fake_conn = FakeConnection()

TEST_ITERATIONS = 100
OBJS_NUM = 1
STARTING_DEPTH = 5
MAX_DEPTH = 5
MAX_ELEMENTS_NUM = 3
STOP_DATA_GENERATION = 0.6
STOP_PATH_GENERATION = 0.1
NONE_PROBABILITY = 0.1

objs = []
fake_objs = []

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
	data = None
	while not isinstance(data, list) and not isinstance(data, dict):
		data = getRandomData(depth)
	return data

def getRandomPath(root):
	if random.random() < STOP_PATH_GENERATION:
		return []

	if isinstance(root, list):
		if len(root) == 0:
			return [None]
		else:
			random_index = random.randrange(len(root))
			return [random_index] + getRandomDefinitePath(root[random_index])
	elif isinstance(root, dict) and len(root) > 0:
		random_key = random.choice(list(root.keys()))
		return [random_key] + getRandomDefinitePath(root[random_key])
	else:
		return []

def getRandomDefinitePath(root):
	path = [None]
	while None in path:
		path = getRandomPath(root)
	return path

def getRandomDefiniteNonTrivialPath(root):
	path = []
	while path == []:
		path = getRandomDefinitePath(root)
	return path

def getRandomInsertPath(root):
	path = []
	while len(path) == 0 or isinstance(path[-1], str):
		path = getRandomPath(root)
	return path

def listInData(data):
	if isinstance(data, list):
		return True
	elif isinstance(data, dict):
		for key in data:
			if listInData(data[key]): return True
		return False
	else:
		return False


class RandomAction:

	def __init__(self, obj_contents):
		self._obj_contents = obj_contents

		args_constructors = {
			'modify': self._constructModifyArgs,
			'insertMany': self._constructInsertArgs,
			'deleteMany': self._constructDeleteArgs
		}

		if self._obj_contents.__class__ not in [list, dict] or len(self._obj_contents) == 0:
			del args_constructors['deleteMany']
		if not listInData(self._obj_contents):
			del args_constructors['insertMany']

		self._args = ()
		self._kwds = {}
		self._method = random.choice(list(args_constructors.keys()))
		args_constructors[self._method]()

	def _constructModifyArgs(self):
		self._args = (getRandomData(MAX_DEPTH),
			getRandomDefinitePath(self._obj_contents))

	def _constructInsertArgs(self):
		self._args = (getRandomInsertPath(self._obj_contents),
			[getRandomData(MAX_DEPTH)])

	def _constructDeleteArgs(self):
		self._args = ([getRandomDefiniteNonTrivialPath(self._obj_contents)],)

	def __call__(self, conn, obj_id):
		args = self._args
		kwds = self._kwds
		getattr(conn, self._method)(obj_id, *args, **kwds)

	def __str__(self):
		return self._method + repr(self._args)


random.seed()

# create objects
for i in range(OBJS_NUM):
	data = getRandomNonTrivialData(STARTING_DEPTH)
	print("Initial state: " + repr(data))

	try:
		objs.append(conn.create(data))
	except:
		print("Error creating object: " + str(data))
		raise

	fake_objs.append(fake_conn.create(data))

# perform test
for c in range(TEST_ITERATIONS):
	for i in range(OBJS_NUM):
		#conn._engine.dump()
		fake_state_before = copy.deepcopy(fake_conn.read(fake_objs[i]))
		try:
			state_before = conn.read(objs[i])
		except:
			print("Error reading object " + str(fake_state_before))
			conn._engine.dump()
			raise

		action = RandomAction(state_before)

		action(fake_conn, fake_objs[i])

		fake_state_after = fake_conn.read(fake_objs[i])
		if fake_state_after is None:
			fake_conn.modify(fake_objs[i], fake_state_before)
			continue

		try:
			action(conn, objs[i])
		except:
			print("Error performing action: " + str(action))
			print("On object: " + str(fake_state_before))
			conn._engine.dump()
			raise

		print(action)

		try:
			state_after = conn.read(objs[i])
		except:
			print("Error reading object: " + str(fake_state_after))
			print("After action: " + str(action))
			print("On state: " + str(fake_state_before))
			conn._engine.dump()
			raise

		if state_after != fake_state_after:
			print("Action results are different:")
			print("State before: " + repr(fake_state_before))
			print("Action: " + str(action))
			print("Main state after: " + repr(state_after))
			print("Fake state after: " + repr(fake_state_after))
			conn._engine.dump()
			raise Exception("Functionality error")
