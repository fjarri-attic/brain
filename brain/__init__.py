import sys, os.path
scriptdir, scriptfile = os.path.split(sys.argv[0])
sys.path.append(os.path.join(scriptdir, ".."))

from brain.facade import connect
from brain.interface import StructureError, LogicError, FormatError, FacadeError
import brain.op as op
from brain.engine import getEngineTags