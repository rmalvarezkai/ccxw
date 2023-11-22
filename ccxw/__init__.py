"""
CCXW - CryptoCurrency eXchange Websocket Library
Main Class Ccxw

Author: Ricardo Marcelo Alvarez
Date: 2023-10-31
"""

from os.path import dirname, basename, isfile, join
import glob
import sys
import importlib.metadata

ccxw_metadata = importlib.metadata.metadata('ccxw')

__title__ = ccxw_metadata['Name']
__summary__ = ccxw_metadata['Summary']
__uri__ = ccxw_metadata['Home-page']
__version__ = ccxw_metadata['Version']
__author__ = ccxw_metadata['Author']
__email__ = ccxw_metadata['Author-email']
__license__ = ccxw_metadata['License']
__copyright__ = 'Copyright © 2023 Ricardo Marcelo Alvarez'

modules = glob.glob(join(dirname(__file__), "*.py"))
__all__ = []

if isinstance(sys.path,list):
    sys.path.append(dirname(__file__))

for f in modules:
    if isfile(f) and not f.endswith('__init__.py'):
        __all__.append(basename(f)[:-3])

from .ccxw import Ccxw
from .binance import BinanceCcxwAuxClass
from .bingx import BingxCcxwAuxClass
from .bybit import BybitCcxwAuxClass
from .kucoin import KucoinCcxwAuxClass
from .okx import OkxCcxwAuxClass





