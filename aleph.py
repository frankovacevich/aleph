"""

"""

import importlib
import sys
program = sys.argv[1]
program = program.replace("/", ".").replace(".py", "")
importlib.import_module(program)
