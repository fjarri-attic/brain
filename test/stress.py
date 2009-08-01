import random
import string

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
STARTING_DEPTH = 3
MAX_DEPTH = 3
MAX_ELEMENTS_NUM = 3
STOP_DATA_GENERATION = 0.1
STOP_PATH_GENERATION = 0.1

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

	if isisntance(root, list):
		random_index = random.randrange(len(root))
		return [random_index] + getRandomPath(root[random_index])
	elif isinstance(root, dict):
		random_key = random.choice(root)
		return [random_key] + gerRandomPath(root[random_key])
	else:
		return []

def getRandomNonTrivialPath(root):
	path = []
	while path == []:
		path = getRandomPath(root)
	return path

def getRandomPathToList(root):
	path = ['aaa']
	while not isinstance(path[-1], int):
		path = getRandomNonTrivialPath(root)

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

		if not listInData(self._obj_contents):
			del args_constructors['insertMany']

		self._args = ()
		self._kwds = {}
		self._method = random.choice(args_constructors)
		args_constructors[self._method]()

	def _constructModifyArgs(self):
		self._args = (getRandomNonTrivialPath(self._obj_contents),
			getRandomData(MAX_DEPTH))

	def _constructInsertArgs(self):
		self._args = (getRandomPathToList(self._obj_contents),
			getRandomData(MAX_DEPTH))

	def _constructDeleteArgs(self):
		self._args = (getRandomNonTrivialPath(self._obj_contents),)

	def __call__(self, conn, obj_id):
		args = self._args
		kwds = self._kwds
		getattr(conn, self._method)(obj_id, *args, **kwds)


random.seed()

# create objects
for i in range(OBJS_NUM):
	data = getRandomNonTrivialData(STARTING_DEPTH)
	print("=" * 80)
	print(data)
	objs.append(conn.create(data))
	fake_objs.append(fake_conn.create(data))

# perform test
for c in range(TEST_ITERATIONS):
	for i in range(OBJS_NUM):
		state_before = conn.read(objs[i])
		fake_state_before = fake_conn.read(fake_objs[i])

		action = RandomAction(state_before)
		action(conn, objs[i])
		action(fake_conn, fake_objs[i])

		state_after = conn.read(objs[i])
		fake_state_after = fake_conn.read(fake_objs[i])

		if state_after != fake_state_after:
			print("Failed")
			print("Main state before: " + repr(state_before))
			print("Fake state before: " + repr(fake_state_before))
			print("Action: " + repr(action))
			print("Main state after: " + repr(state_after))
			print("Fake state after: " + repr(fake_state_after))
			break
