import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

from db import interface, database, engine
import yaml


class Facade:

	def connect(self, path, open_existing=None):

		return Connection(database.SimpleDatabase(
			engine.Sqlite3Engine, path, open_existing))


class Connection:

	def __init__(self, db):
		self.db = db
		self.transaction = False
		self.requests = []

	def disconnect(self):
		self.db.disconnect()

	def begin(self):
		self.transaction = True

	def commit(self):
		try:
			self.db.processRequests(requests)
		finally:
			self.transaction = False
			self.requests = []

	def rollback(self):
		self.transaction = False
		self.requests = []

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

		if not 'type' in request.keys():
			raise Exception("Request type is missing")

		if not request['type'] in handlers.keys():
			raise Exception("Unknown request type: " + str(request['type']))

		return handlers[request['type']](request)

	def processConnectRequest(self, request):
		if not 'path' in request.keys():
			raise Exception('Database path is missing')
		path = request['path']

		open_existing = request['connect'] if 'connect' in request.keys() else None

		self.session_counter += 1
		self.sessions[self.session_counter] = self.facade.connect(path, open_existing)

		return self.session_counter

	def processDisonnectRequest(self, request):
		if not 'session' in request.keys():
			raise Exception('Session ID is missing')
		session = request['session']

		self.sessions[session].disconnect()
		del self.sessions[session]


if __name__ == '__main__':
	ff = Facade()
	f = YamlFacade(ff)

	s = f.parseRequest('''
type: connect
path: 'c:\\gitrepos\\brain\\parse\\test.dat'
''')

	print("Opened session: " + str(s))

	f.parseRequest('''
type: disconnect
session: {session}
'''.format(session=s))

	print("Closed session: " + str(s))