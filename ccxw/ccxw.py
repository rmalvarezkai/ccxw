"""
CCXW - CryptoCurrency eXchange Websocket Library
Main Class Ccxw

Author: Ricardo Marcelo Alvarez
Date: 2023-10-31
"""

import threading
import os
import os.path
import time
import datetime
import json
import sqlite3
import base64
import random
import gzip
import importlib
import websocket

class Ccxw(): # pylint: disable=too-many-instance-attributes
    """
    CCXW - CryptoCurrency eXchange Websocket Library
    ================================================
    This class retrieves data from exchange WebSocket APIs and inserts or updates data
    in a temporary SQLite database, which is done in the background, making the data
    available for retrieval.
    Currently, it is available for the following exchanges: Binance, Bybit, etc.
    For a complete list, see Ccxw.get_supported_exchanges().
    It supports only the following endpoints: order_book, kline, etc.
    For a complete list, see Ccxw.get_supported_endpoints().

    Example:

    ```python
    import time
    import pprint
    from ccxw import Ccxw

    wsm = ccxw.Ccxw('binance', 'order_book', 'BTC/USDT', result_max_len=20)  # Create instance

    wsm.start()  # Start getting data

    time.sleep(2)  # Wait for available data

    for i in range(0, 10):
        data = wsm.get_current_data()
        pprint.pprint(data, sort_dicts=False)
        time.sleep(1)

    wsm.stop()  # Stop getting data
    ```
    """

    # pylint: disable=too-many-arguments, too-many-branches, too-many-statements
    def __init__(self, exchange, endpoint=None, symbol=None, trading_type: str='SPOT',\
        testmode=False, api_key: str=None, api_secret: str=None, result_max_len: int=5,\
        update_speed: str='100ms', interval: str='1m', data_max_len: int=400, debug: bool=False):
        """
        Ccxw constructor
        ================
            :param self: Ccxw instance.
            :param exchange: str exchange name.
            :param endpoint: str only allowed 'order_book' | 'kline' | 'trades' | 'ticker'.
            :param symbol: str unified symbol.
            :param trading_type: str only allowed 'SPOT'.
            :param testmode: bool.
            :param api_key: str Not necessary only for future features.
            :param api_secret: str Not necessary only for future features.
            :param result_max_len: int Max return values > 1 and <= data_max_len.
            :param update_speed: str only allowed '100ms' | '1000ms' Only for some endpoints.
            :param interval: str only allowed '1m' | '3m' | '5m' | '15m' | '30m' | '1H' | '2H' 
                | '4H' | '6H' | '8H' | '12H' | '1D' | '3D' | '1W' | '1M'.
            :param data_max_len: int. > 1 and <= 400 max len of data getting from exchange.
            :param debug: bool Output verbosity.

            :return: Return a new instance of the Class Ccxw.
        """

        self.__debug = debug
        self.__data_max_len = data_max_len

        self.__data_max_len = min(self.__data_max_len, 400)
        self.__data_max_len = max(self.__data_max_len, 1)

        if exchange not in Ccxw.get_supported_exchanges():
            raise ValueError('The exchange ' + str(exchange) + ' is not supported.')

        if endpoint not in Ccxw.get_supported_endpoints():
            raise ValueError('The endpoint ' + str(endpoint) + ' is not supported.')

        self.__key_sel = 'SEL_' + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f"))\
            + '_' + str(random.randint(90000,99999)) + '_' + str(random.randint(90000,99999))

        if symbol is not None:
            self.__key_sel = self.__key_sel + symbol.replace("/","").upper()

        if not (isinstance(symbol,str) and len(symbol) > 0):
            raise ValueError('Bad symbol: ' + str(symbol))

        self.__exchange = exchange
        self.__ws = None
        self.__ws_url = None
        self.__ws_endpoint = endpoint
        self.__ws_endpoint_url = None
        self.__ws_endpoint_on_auth_vars = None
        self.__ws_endpoint_on_open_vars = None
        self.__ws_endpoint_on_close_vars = None
        self.__ws_ping_interval = 0
        self.__ws_ping_timeout = None
        self.__ws_symbol = symbol
        self.__socket = None
        self.__thread = None
        self.__stop_launcher = False # Used in methods self.start() and self.stop()
        self.__conn_db = None
        self.__cursor_db = None
        self.__ws_temp_data = None
        self.min_proc_time_ms = None
        self.max_proc_time_ms = 0

        self.__result_max_len = result_max_len
        self.__update_speed = update_speed
        self.__interval = interval
        self.__ws_ended = True

        if exchange in self.get_supported_exchanges():

            self.__api_key = api_key
            self.__api_secret = api_secret
            self.__testmode = testmode

            self.__trading_type = trading_type

            self.__auxiliary_class = getattr(importlib.import_module\
                (self.__exchange, package=self.__exchange),\
                self.__exchange.capitalize() + 'CcxwAuxClass')\
                (endpoint=self.__ws_endpoint, symbol=self.__ws_symbol, testmode=self.__testmode,\
                api_key=self.__api_key, api_secret=self.__api_secret,\
                result_max_len=self.__result_max_len,\
                update_speed=self.__update_speed, interval=self.__interval,\
                data_max_len=self.__data_max_len, debug=self.__debug)

            self.__api_keys = {} #Reserved for future features

            if api_key is not None:
                self.__api_keys['api_key'] = api_key

            if api_secret is not None:
                self.__api_keys['api_secret'] = api_secret

            if not self.__auxiliary_class.if_symbol_supported():
                raise ValueError('Symbol ' + str(symbol) + ' have not '\
                + str(self.__trading_type.lower())\
                + ' market in ' + str(exchange) + ' exchange.')

            self.__database_name = '/tmp/temp_database_'\
                + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f"))\
                + '_' + str(random.randint(90000,99999)) + '_'\
                + str(random.randint(90000,99999)) + '.db'

            self.__table_name = 'temp_table'

            #self.__conn_db = sqlite3.connect(self.__database_name, check_same_thread=False)
            self.__conn_db = sqlite3.connect(':memory:', check_same_thread=False)

            self.__result_max_len = min(self.__result_max_len, self.__data_max_len)
            self.__result_max_len = max(self.__result_max_len, 1)

            self.__ws_url = self.__auxiliary_class.get_websocket_url()

            if isinstance(self.__ws_url,str) and len(self.__ws_url) > 0:

                if self.__ws_endpoint in Ccxw.get_supported_endpoints():
                    __get_websocket_endpoint_data = (
                        self.__auxiliary_class.get_websocket_endpoint_path()
                    )
                    if __get_websocket_endpoint_data is not None\
                        and isinstance(__get_websocket_endpoint_data,dict):
                        if 'ws_endpoint_url' in __get_websocket_endpoint_data:
                            self.__ws_endpoint_url = (
                                __get_websocket_endpoint_data['ws_endpoint_url']
                            )

                        if 'ws_endpoint_on_open_vars' in __get_websocket_endpoint_data:
                            self.__ws_endpoint_on_open_vars = (
                                __get_websocket_endpoint_data['ws_endpoint_on_open_vars']
                            )

                        if 'ws_endpoint_on_close_vars' in __get_websocket_endpoint_data:
                            self.__ws_endpoint_on_close_vars = (
                                __get_websocket_endpoint_data['ws_endpoint_on_close_vars']
                            )

                        self.__socket = self.__ws_url + self.__ws_endpoint_url

                        if 'ws_ping_interval' in __get_websocket_endpoint_data:
                            self.__ws_ping_interval = (
                                __get_websocket_endpoint_data['ws_ping_interval']
                            )

                        if 'ws_ping_timeout' in __get_websocket_endpoint_data:
                            self.__ws_ping_timeout = (
                                __get_websocket_endpoint_data['ws_ping_timeout']
                            )

                        if 'ws_endpoint_on_auth_vars' in __get_websocket_endpoint_data:
                            self.__ws_endpoint_on_auth_vars = (
                                __get_websocket_endpoint_data['ws_endpoint_on_auth_vars']
                            )
                    else:
                        raise ValueError('Supported endpoints '\
                            + str(Ccxw.get_supported_endpoints()))
                else:
                    raise ValueError('Supported endpoints ' + str(Ccxw.get_supported_endpoints()))
            else:
                raise ValueError('The exchange ' + str(exchange) + ' have not websocket api.')

        else:
            raise ValueError('The exchange ' + str(exchange) + ' have not websocket api.')

    def __del__(self):

        if not self.__stop_launcher:
            self.stop()

        if hasattr(self,'__conn_db') and self.__conn_db is not None:
            self.__conn_db.commit()
            self.__conn_db.close()

        if hasattr(self,'__database_name') and self.__database_name is not None:
            if os.path.exists(self.__database_name):
                os.remove(self.__database_name)

    def get_exchange_info(self, full_list=False):
        """
        get_exchange_info
        =================
            This function get exchange info. 
                :param full_list: bool.
                :return dict: Return exchange info.
        """

        result = None

        result = self.__auxiliary_class.get_exchange_info(full_list)

        return result

    def get_exchange_full_list_symbols(self, sort_list=True):
        """
        get_exchange_full_list_symbols
        ==============================
            This function get exchange info. 
                :param sort_list: bool.
                :return dict: Return exchange info.
        """

        result = None

        result = self.__auxiliary_class.get_exchange_full_list_symbols(sort_list)

        return result

    def start(self):
        """
        Starting getting data form exchange
        ===================================
            This method start a websocket client in a new thread.
                :param self: Ccxw instance.

                :return bool: Return True if starting OK or False 
        """
        result = False

        try:
            self.__stop_launcher = False # Used in self.stop()
            self.__thread = threading.Thread(target=self.__websocket_launcher,args=(self.__socket,))
            self.__thread.start()
            result = True
        except Exception: # pylint: disable=broad-except
            result = False

        time.sleep(5)

        return result

    def stop(self,time_to_wait=5):
        """
        Stopping getting data form exchange
        ===================================
            This method stop a websocket client and join and wait the thread ended.
                :param self: Ccxw instance.
                :param time_to_wait: int Max time to wait the websocket ended.

                :return bool: Return True if starting OK or False 
        """
        result = False

        __time_counter = 0
        __time_limit = 90

        if self.__ws and self.__ws_endpoint_on_close_vars is not None\
            and isinstance(self.__ws_endpoint_on_close_vars,str)\
                and len(self.__ws_endpoint_on_close_vars) > 0:
            self.__ws.send(self.__ws_endpoint_on_close_vars)

        time.sleep(2)

        self.__stop_launcher = True

        if not self.__ws_ended:
            try:
                self.__ws.close()
                result = True
            except Exception: # pylint: disable=broad-except
                result = False

        while not self.__ws_ended and __time_counter <= __time_limit:
            __time_counter = __time_counter + 1
            time.sleep(1)

        if not self.__ws_ended:
            try:
                self.__ws.close()
                result = True
            except Exception: # pylint: disable=broad-except
                result = False
        else:
            result = True

        if self.__thread is not None:
            self.__thread.join(time_to_wait)

        if hasattr(self.__auxiliary_class, 'stop'):
            self.__auxiliary_class.stop()

        del self.__auxiliary_class

        return result

    def __websocket_launcher(self,socket):
        """
        Initialize temporal database and starting getting data from websocket
        ======================================================================
            This is a Class internal method and running in a new thread,
            First initialize a temporal database and then initialize and launch websocket client
                :param self: Ccxw instance.
                :param str: websocket URL.

                :return bool: Return True if starting OK or False 
        """
        result = False

        try:
            self.__cursor_db = self.__conn_db.cursor()
            __sql_create_table_db = 'DROP TABLE IF EXISTS ' + str(self.__table_name) + ';\n'
            __sql_create_table_db = __sql_create_table_db + 'CREATE TABLE IF NOT EXISTS '
            __sql_create_table_db = __sql_create_table_db + str(self.__table_name)
            __sql_create_table_db = __sql_create_table_db + '(key_data VARCHAR(40) NOT NULL'
            __sql_create_table_db = __sql_create_table_db + ' PRIMARY KEY, value_data TEXT NULL);\n'
            __sql_create_table_db = __sql_create_table_db + 'INSERT INTO ' + str(self.__table_name)
            __sql_create_table_db = __sql_create_table_db + ' (key_data,value_data) VALUES ("'
            __sql_create_table_db = __sql_create_table_db + str(self.__key_sel) + '",NULL);\n'
            self.__cursor_db.executescript(__sql_create_table_db)
            self.__conn_db.commit()
            result = True
        except Exception as exc: # pylint: disable=broad-except
            result = False
            print(str(exc))

        websocket.enableTrace(self.__debug)

        try:
            self.__ws = (
                websocket.WebSocketApp(socket, on_message=self.__manage_websocket_message,\
                                       on_open=self.__manage_websocket_open,\
                                       on_close=self.__manage_websocket_close)
            )
            result = True

        except Exception as exc: # pylint: disable=broad-except
            result = False
            print('On create websocket exception: ' + str(exc))
        self.__ws_ended = False
        __ws_temp = (
            self.__ws.run_forever(ping_interval=self.__ws_ping_interval,\
                                  ping_timeout=self.__ws_ping_timeout, reconnect=320)
            )

        self.__ws_ended = True

        return result

    def __manage_websocket_open(self,ws):
        """
        websocket.WebSocketApp on_open function.
        ========================================
            Some exchange endpoints require send specific data when websocket is opened 
                :param self: Ccxw instance.
                :param ws: websocket.WebSocketApp instance.

                :return None:
        """
        result = False

        if self.__ws_endpoint_on_auth_vars is not None\
            and isinstance(self.__ws_endpoint_on_auth_vars,str):
            try:
                ws.send(self.__ws_endpoint_on_auth_vars)
                result = True
            except Exception: # pylint: disable=broad-except
                result = False

        elif self.__ws_endpoint_on_open_vars is not None\
            and isinstance(self.__ws_endpoint_on_open_vars,str):
            try:
                ws.send(self.__ws_endpoint_on_open_vars)
                result = True
            except Exception: # pylint: disable=broad-except
                result = False
        else:
            result = True

        return result

    def __manage_websocket_close(self,ws,close_status_code,close_msg):
        """
        websocket.WebSocketApp on_close function.
        =========================================
            Some exchange endpoints require send specific data when websocket is closed
                :param self: Ccxw instance.
                :param ws: websocket.WebSocketApp instance.
                :param close_status_code: int close status code.
                :param close_msg: str close message.

                :return None:
        """

        __tmp_code = close_status_code
        __tmp_msg = close_msg

        if ws is not None:
            self.__ws_ended = True
            time.sleep(5)


    def ws_on_error(self, ws,error):
        """
        websocket.WebSocketApp on_error function.
        =========================================
            :param self: Ccxw instance.
            :param ws: websocket.WebSocketApp instance.
            :param error: Exception object.

            :return None: 
        """
        err_msg = "Found error in websocket connection "\
            + str(ws.url)  + " -> WS: " + str(ws)\
            + ", error: " + str(error)
        print(err_msg)

    def __manage_websocket_message(self,ws,message_in):
        """
        websocket.WebSocketApp on_message function.
        ===========================================
            This function process the data. The data is converted in json string,
            compress, encode in base64 and then insert or update in temporal database
                :param self: Ccxw instance.
                :param ws: websocket.WebSocketApp instance.
                :param message_in: str message.

                :return None:
        """

        __time_ini = time.time_ns()

        try:
            __managed_data = self.__auxiliary_class.manage_websocket_message(ws,message_in)

            if __managed_data is not None and isinstance(__managed_data,dict)\
                and 'data' in __managed_data and 'max_proc_time_ms' in __managed_data:
                self.__ws_temp_data = __managed_data
                self.__ws_temp_data['min_proc_time_ms'] = self.min_proc_time_ms
                self.__ws_temp_data['max_proc_time_ms'] = self.max_proc_time_ms
                __message_out = json.dumps(self.__ws_temp_data)
                __message_out = gzip.compress(bytes(__message_out,'utf-8'), compresslevel=9)
                __message_base64 = base64.b64encode(__message_out).decode("ascii")
                __sql_update = 'UPDATE ' + str(self.__table_name) + ' SET value_data = "'\
                    + str(__message_base64) + '" WHERE  key_data = "' + str(self.__key_sel) + '" ;'
                self.__cursor_db = self.__conn_db.cursor()
                self.__cursor_db.execute(__sql_update)
                self.__conn_db.commit()

        except Exception as exc: # pylint: disable=broad-except
            print(str(exc))

        __time_diff = time.time_ns() - __time_ini
        __time_diff = round((__time_diff / 1000000),3)

        if __time_diff > self.max_proc_time_ms:
            self.max_proc_time_ms = __time_diff

        if self.min_proc_time_ms is None or __time_diff < self.min_proc_time_ms:
            self.min_proc_time_ms = __time_diff

        if self.__stop_launcher and not self.__ws_ended:
            ws.close()

    def get_current_data(self):
        """
        Ccxw get_current_data function.
        ===========================================
            This function get de data from temporal database decode from base64,
            decompress and convert json data in dict and then return this data
                :param self: Ccxw instance.

                :return: dict with last data.
        """
        result = None

        __sql_select = 'SELECT value_data FROM "' + str(self.__table_name)\
            + '" WHERE  key_data = "' + str(self.__key_sel) + '" ;'
        try:
            __local_cursor_db = self.__conn_db.cursor()
            __local_cursor_db.execute(__sql_select)
            __current_data = __local_cursor_db.fetchone()

            if __current_data is not None\
                and isinstance(__current_data,(list, tuple))\
                and len(__current_data) > 0 and __current_data[0] is not None\
                and isinstance(__current_data[0],str):
                result = (
                    json.loads(gzip.decompress(base64.b64decode(__current_data[0])).decode('utf-8'))
                )
        except Exception as exc: # pylint: disable=broad-except
            print(str(exc))

        return result

    @classmethod
    def get_supported_exchanges(cls):
        """
        Ccxw get_supported_exchanges function.
        ======================================
            This method return a list of supported exchanges.
                :param cls: Ccxw Class.

                :return: list of supported exchanges.
        """
        __suported_exchanges = ['binance' , 'bybit', 'kucoin', 'bingx','okx']

        return __suported_exchanges

    @classmethod
    def get_supported_endpoints(cls):
        """
        Ccxw get_supported_endpoints function.
        ======================================
            This method return a list of supported end points.
                :param cls: Ccxw Class.

                :return: list of supported endpoints.
        """
        __suported_endpoints = ['order_book', 'kline', 'trades','ticker']

        return __suported_endpoints
