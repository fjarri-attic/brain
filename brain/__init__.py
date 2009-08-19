import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

from brain.connection import connect
from brain.interface import BrainError, StructureError, LogicError, FormatError, FacadeError
import brain.op as op
from brain.engine import getEngineTags, getDefaultEngineTag
from brain.xmlrpclayer import BrainXMLRPCError, Server, Client
