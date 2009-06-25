import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

from db import interface, database
import yaml

class Facade:

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
		print("Open request")

	def processCloseRequest(self, request):
		print("Close request")

if __name__ == '__main__':
	f = Facade()
	f.parseYamlRequest(
'''
type: open
''')
	f.parseYamlRequest(
'''
type: close
''')
