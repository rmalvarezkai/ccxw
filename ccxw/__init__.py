
from os.path import dirname, basename, isfile, join
import glob, sys, importlib

modules = glob.glob(join(dirname(__file__), "*.py"))
__all__ = []

if isinstance(sys.path,list):
    sys.path.append(dirname(__file__))

for f in modules:
    if isfile(f) and not f.endswith('__init__.py'):
        __all__.append(basename(f)[:-3])
