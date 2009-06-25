import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

from db import interface, database, engine
import yaml

class Facade:

	def __init__(self):
		self.sessions = {}
		self.session_counter = 0

	def parseYamlRequest(self, request):
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
		if not 'name' in request.keys():
			raise Exception('Database name is missing')
		name = request['name']

		open_existing = request['open'] if 'open' in request.keys() else None

		self.session_counter += 1
		self.sessions[self.session_counter] = database.SimpleDatabase(
			engine.Sqlite3Engine, name)

		return self.session_counter

	def processCloseRequest(self, request):
		if not 'session' in request.keys():
			raise Exception('Session ID is missing')
		session = request['session']

		self.sessions[session] = None

if __name__ == '__main__':
	f = Facade()
	s = f.parseYamlRequest(
'''
type: open
name: ':memory:'
''')

	print("Opened session: " + str(s))

	f.parseYamlRequest(
'''
type: close
session: {session}
'''.format(session=s))

	print("Closed session: " + str(s))