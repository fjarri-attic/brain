import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

from db import interface, database, engine
import yaml


class Facade:

	def __init__(self):
		self.sessions = {}
		self.session_counter = 0

	def openSession(self, path, open_existing=None):

		self.session_counter += 1
		self.sessions[self.session_counter] = database.SimpleDatabase(
			engine.Sqlite3Engine, path, open_existing)

		return self.session_counter

	def closeSession(self, session):

		self.sessions[session].close()
		del self.sessions[session]


class YamlFacade:

	def __init__(self, facade):
		self.facade = facade

	def parseRequest(self, request):
		request = yaml.load(request)

		handlers = {
			'open': self.processOpenRequest,
			'close': self.processCloseRequest
		}

		if not 'type' in request.keys():
			raise Exception("Request type is missing")

		if not request['type'] in handlers.keys():
			raise Exception("Unknown request type: " + str(request['type']))

		return handlers[request['type']](request)

	def processOpenRequest(self, request):
		if not 'path' in request.keys():
			raise Exception('Database path is missing')
		path = request['path']

		open_existing = request['open'] if 'open' in request.keys() else None

		return self.facade.openSession(path, open_existing)

	def processCloseRequest(self, request):
		if not 'session' in request.keys():
			raise Exception('Session ID is missing')
		session = request['session']

		self.facade.closeSession(session)


if __name__ == '__main__':
	ff = Facade()
	f = YamlFacade(ff)

	s = f.parseRequest('''
type: open
path: 'c:\\gitrepos\\brain\\parse\\test.dat'
''')

	print("Opened session: " + str(s))

	f.parseRequest('''
type: close
session: {session}
'''.format(session=s))

	print("Closed session: " + str(s))