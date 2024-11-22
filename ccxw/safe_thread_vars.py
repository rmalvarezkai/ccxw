"""
Ccxw - CryptoCurrency eXchange Websocket Library
Thread safe dict

Author: Ricardo Marcelo Alvarez
Date: 2023-10-31
"""

from threading import Lock

class DictSafeThread():
    """
    Thread safe dict
    ===================================================================
        This class contains helper functions for the Ccxw class.
    """

    def __init__(self, dict_in=None):
        self.__lock = Lock()
        self._data = None

        with self.__lock:
            if dict_in is None:
                self._data = {}
            elif isinstance(dict_in, dict):
                self._data = dict_in
            else:
                raise ValueError('Input data is not None or dict')

    def __del__(self):
        self._data = None

    def __enter__(self):
        self.__lock.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self.__lock.__exit__(exc_type, exc_value, traceback)

    def __str__(self):
        result = None
        with self.__lock:
            result = str(self._data)

        return result

    def __repr__(self):

        result = None
        with self.__lock:
            result = repr(self._data)

        return result

    def __getitem__(self, key):
        result = None

        with self.__lock:
            if key in self._data:
                result = self._data[key]

        return result

    def __setitem__(self, key, value):
        result = None

        with self.__lock:
            self._data[key] = value

        return result

    def __delitem__(self, key):
        result = None

        with self.__lock:
            del self._data[key]

        return result

    def __len__(self):
        result = 0

        with self.__lock:
            result = len(self._data)

        return result

    def __iter__(self):
        result = None

        with self.__lock:
            result = self._data.keys()
            result = iter(result)

        return result

    def __contains__(self, key):
        result = False

        with self.__lock:
            result = key in self._data

        return result

    def __eq__(self, other):
        result = False

        with self.__lock:
            if isinstance(other, DictSafeThread):
                result = self._data == other._data

        return result

    def __ne__(self, other):
        result = False
        result = not self.__eq__(other)
        return result

    def __hash__(self):
        result = ''

        with self.__lock:
            result = hash(self.__lock)

        return hash(result)
