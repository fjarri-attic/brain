import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

from db import interface, database, engine
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


class Facade:

	def connect(self, path, open_existing=None):

		return Connection(database.SimpleDatabase(
			engine.Sqlite3Engine, path, open_existing))


def transacted(func):
	def handler(obj, *args, **kwds):
		create_transaction = not obj.transaction

		if create_transaction: obj.begin()
		func(obj, *args, **kwds)
		if create_transaction:
			return [obj.commit()]

	return handler


class Connection:

	def __init__(self, db):
		self.db = db
		self.transaction = False
		self.requests = []

	def disconnect(self):
		self.db.disconnect()

	def begin(self):
		if not self.transaction:
			self.transaction = True
		else:
			raise Exception("Transaction is already in progress")

	def commit(self):
		try:
			return self.db.processRequests(self.requests)
		finally:
			self.transaction = False
			self.requests = []

	def rollback(self):
		if self.transaction:
			self.transaction = False
			self.requests = []
		else:
			raise Exception("Transaction is not in progress")

	@transacted
	def modify(self, id, value, path=None):
		if path is None: path = []
		parsed = flattenHierarchy(value)
		fields = [interface.Field(path + relative_path, val)
			for relative_path, val in parsed]
		self.requests.append(interface.ModifyRequest(id, fields))

	@transacted
	def read(self, id, path=None):
		if path is not None:
			path = interface.Field(path)
		self.requests.append(interface.ReadRequest(id, path))


class YamlFacade:

	def __init__(self, facade):
		self.facade = facade
		self.sessions = {}
		self.session_counter = 0

	def parseRequest(self, request):
		request = yaml.load(request)

		handlers = {
			'connect': self.processConnectRequest,
			'disconnect': self.processDisonnectRequest
		}

		if not 'type' in request:
			raise Exception("Request type is missing")

		if not request['type'] in handlers:
			raise Exception("Unknown request type: " + str(request['type']))

		return handlers[request['type']](request)

	def processConnectRequest(self, request):
		if not 'path' in request:
			raise Exception('Database path is missing')
		path = request['path']

		open_existing = request['connect'] if 'connect' in request else None

		self.session_counter += 1
		self.sessions[self.session_counter] = self.facade.connect(path, open_existing)

		return self.session_counter

	def processDisonnectRequest(self, request):
		if not 'session' in request:
			raise Exception('Session ID is missing')
		session = request['session']

		self.sessions[session].disconnect()
		del self.sessions[session]


if __name__ == '__main__':
	f = Facade()
	c = f.connect('c:\\gitrepos\\brain\\parse\\test.dat')

	c.modify('1', 'RRR', ['name'])

	print(c.read('1'))

	c.disconnect()
