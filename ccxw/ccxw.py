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
import humanize
import websocket

from ccxw.binance import BinanceCcxwAuxClass
from ccxw.bybit import BybitCcxwAuxClass
from ccxw.bingx import BingxCcxwAuxClass
from ccxw.kucoin import KucoinCcxwAuxClass
from ccxw.okx import OkxCcxwAuxClass
from ccxw.binanceus import BinanceusCcxwAuxClass

class CcxwExchangeConfig:
    """
    A configuration class for managing exchange classes.

    Attributes:
    exchange_classes (dict): A dictionary mapping exchange names to their corresponding classes.

    Example:
    To obtain the class for the 'binance' exchange, use ExchangeConfig.exchange_classes['binance'].
    """

    exchange_classes = {
        'binance': BinanceCcxwAuxClass,
        'bybit': BybitCcxwAuxClass,
        'bingx': BingxCcxwAuxClass,
        'kucoin': KucoinCcxwAuxClass,
        'okx': OkxCcxwAuxClass,
        'binanceus': BinanceusCcxwAuxClass
    }

class Ccxw():
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

    symbol = 'BTC/USDT'

    streams = [\
                    {\
                        'endpoint': 'order_book',\
                        'symbol': symbol
                    },\
                    {\
                        'endpoint': 'kline',\
                        'symbol': symbol,\
                        'interval': interval\
                    },\
                    {\
                        'endpoint': 'trades',\
                        'symbol': symbol
                    },\
                    {\
                        'endpoint': 'ticker',\
                        'symbol': symbol
                    }\
            ]
    
    wsm = Ccxw('binance',\
                streams,\
                result_max_len=5,\
                data_max_len=10)

    wsm.start()  # Start getting data

    time.sleep(2)  # Wait for available data

    for i in range(0, 10):
        for stream in streams:
            interval = 'none'
            if 'interval' in stream:
                interval = stream['interval']
            data = wsm.get_current_data(stream['endpoint'], stream['symbol'], interval)
            pprint.pprint(data, sort_dicts=False)
            print('----------------------------------')
            time.sleep(1)
        print('============================================================')

    wsm.stop()  # Stop getting data
    ```
    """

    def __init__(self, exchange, streams=list[dict], trading_type: str='SPOT',\
        testmode: bool=False, result_max_len: int=5,\
        data_max_len: int=2500, debug: bool=False):
        """
        Ccxw constructor
        ================
            :param self: Ccxw instance.
            :param exchange: str exchange name.
            :param streams: list[dict]
                                    dicts must have this struct.
                                        {
                                            'endpoint': str only allowed 'order_book' | 'kline' |\
                                                'trades' | 'ticker',
                                            'symbol': str unified symbol.,
                                            'interval': str '1m' | '3m' | '5m' | '15m' | '30m' |\
                                                '1h' | '2h' | '4h' | '6h' | '8h' | '12h' | '1d' |\
                                                '3d' | '1w' | '1mo' for 'kline' endpoint is\
                                                    mandatory.
                                        }
            :param trading_type: str only allowed 'SPOT'.
            :param testmode: bool.
            :param result_max_len: int Max return values > 1 and <= data_max_len.
            :param data_max_len: int. > 1 and <= 2500 max len of data getting from exchange.
            :param debug: bool Output verbosity.

            :return: Return a new instance of the Class Ccxw.
        """

        self.__debug = debug
        self.__data_max_len = data_max_len

        self.__data_max_len = min(self.__data_max_len, 2500)
        self.__data_max_len = max(self.__data_max_len, 1)

        self.__current_ccxw_thread = threading.current_thread()

        if self.__current_ccxw_thread is not None:
            self.__current_ccxw_thread.name = 'class_ccxw_thread'

        if exchange not in Ccxw.get_supported_exchanges():
            raise ValueError('The exchange ' + str(exchange) + ' is not supported.')

        self.__key_sel = {}

        self.__exchange = None
        self.__ws = None
        self.__ws_url = None
        self.__ws_endpoint_url = None
        self.__ws_endpoint_on_auth_vars = None
        self.__ws_endpoint_on_open_vars = None
        self.__ws_endpoint_on_close_vars = None
        self.__ws_ping_interval = 0
        self.__ws_ping_timeout = None
        self.__socket = None
        self.__thread = None
        self.__stop_launcher = False # Used in methods self.start() and self.stop()
        self.__conn_db = None
        self.__cursor_db = None
        self.min_proc_time_ms = None
        self.max_proc_time_ms = 0
        self.__trading_type = 'SPOT'
        self.__start_time = 0

        self.__result_max_len = result_max_len
        self.__ws_ended = True

        self.__ws_streams = streams

        if exchange in self.get_supported_exchanges():
            self.__exchange = exchange
            self.__testmode = testmode

            self.__trading_type = trading_type

            self.__auxiliary_class = CcxwExchangeConfig.exchange_classes[self.__exchange]\
                (streams=self.__ws_streams, trading_type=self.__trading_type,\
                testmode=self.__testmode,\
                result_max_len=self.__result_max_len,\
                data_max_len=self.__data_max_len, debug=self.__debug)

            self.__init_key_selector()

            self.__database_name = '/tmp/temp_database_'\
                + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f"))\
                + '_' + str(random.randint(90000,99999)) + '_'\
                + str(random.randint(90000,99999)) + '.db'

            self.__table_name = 'temp_table'

            #self.__conn_db = sqlite3.connect(self.__database_name, check_same_thread=False)
            self.__conn_db = sqlite3.connect(':memory:', check_same_thread=False)
            self.__conn_db_lock = threading.Lock()

            self.__result_max_len = min(self.__result_max_len, self.__data_max_len)
            self.__result_max_len = max(self.__result_max_len, 1)

            self.__ws_url = self.__auxiliary_class.get_websocket_url()

            if isinstance(self.__ws_url,str) and len(self.__ws_url) > 0:

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
                raise ValueError('The exchange ' + str(exchange) + ' have not websocket api.')

        else:
            raise ValueError('The exchange ' + str(exchange) + ' have not websocket api.')

    def __del__(self):

        if not self.__stop_launcher:
            self.stop()

        # if self.__auxiliary_class is not None and hasattr(self.__auxiliary_class, 'stop'):
        #     self.__auxiliary_class.stop()

        if hasattr(self,'__conn_db') and self.__conn_db is not None:
            with self.__conn_db_lock:
                self.__conn_db.commit()
                self.__conn_db.close()

        if hasattr(self,'__database_name') and self.__database_name is not None:
            if os.path.exists(self.__database_name):
                os.remove(self.__database_name)

        del self.__auxiliary_class

    def __init_key_selector(self):
        result = False

        for stream in self.__ws_streams:
            __index = None
            __interval = 'none'
            if 'interval' in stream:
                __interval = stream['interval']

            __index = self.__auxiliary_class.get_stream_index(stream['endpoint'],\
                                                              stream['symbol'],\
                                                              __interval)

            self.__key_sel[__index] = __index
        return result

    def get_exchange_info(self):
        """
        get_exchange_info
        =================
            This function get exchange info. 
                :return dict: Return exchange info.
        """

        result = None

        result = self.__auxiliary_class.get_exchange_info()

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
        self.__start_time = int(time.time())

        if self.__auxiliary_class is not None and hasattr(self.__auxiliary_class, 'start'):
            self.__auxiliary_class.start()
            time.sleep(2)

        try:
            self.__stop_launcher = False # Used in self.stop()
            self.__thread = threading.Thread(target=self.__websocket_launcher,\
                                             args=(self.__socket,),\
                                             name='ccxw_websocket_thread')
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

        if self.__ws and self.__ws_endpoint_on_close_vars is not None:
            if isinstance(self.__ws_endpoint_on_close_vars, str)\
                and len(self.__ws_endpoint_on_close_vars) > 0:
                self.__ws.send(self.__ws_endpoint_on_close_vars)
            if isinstance(self.__ws_endpoint_on_close_vars, list)\
                and len(self.__ws_endpoint_on_close_vars) > 0:
                for __close_vars in self.__ws_endpoint_on_close_vars:
                    self.__ws.send(__close_vars)

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

        if self.__auxiliary_class is not None and hasattr(self.__auxiliary_class, 'stop'):
            self.__auxiliary_class.stop()

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

        # print('ENTRE: 8888')
        # pprint.pprint(str(self.__auxiliary_class))
        self.__start_time = 0

        return result

    def __websocket_launcher(self, socket):
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
            with self.__conn_db_lock:
                self.__cursor_db = self.__conn_db.cursor()

                __sql_create_table_db = f'DROP TABLE IF EXISTS {self.__table_name};\n'
                __sql_create_table_db += f'CREATE TABLE IF NOT EXISTS {self.__table_name} \
                                            (key_data VARCHAR(40) PRIMARY KEY, value_data TEXT);\n'

                self.__cursor_db.executescript(__sql_create_table_db)

                for __key_data in self.__key_sel.values():
                    __sql_insert_data = f'INSERT INTO {self.__table_name} \
                                            (key_data, value_data) VALUES (?, NULL);\n'
                    self.__cursor_db.execute(__sql_insert_data, (str(__key_data),))

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
                                       on_reconnect=self.__manage_websocket_reconnect,\
                                       on_close=self.__manage_websocket_close)
            )
            result = True

        except Exception as exc: # pylint: disable=broad-except
            result = False
            print('On create websocket exception: ' + str(exc))
        self.__ws_ended = False
        __ws_temp = (
            self.__ws.run_forever(ping_interval=self.__ws_ping_interval,\
                                  ping_timeout=self.__ws_ping_timeout,\
                                  reconnect=140)
            )

        self.__ws_ended = True

        return result

    def __manage_websocket_open(self, ws):
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
            and isinstance(self.__ws_endpoint_on_auth_vars, str):
            try:
                ws.send(self.__ws_endpoint_on_auth_vars)
                result = True
            except Exception: # pylint: disable=broad-except
                result = False

        elif self.__ws_endpoint_on_open_vars is not None:
            if isinstance(self.__ws_endpoint_on_open_vars, str):
                try:
                    ##print('OV: ' + self.__ws_endpoint_on_open_vars)
                    ws.send(self.__ws_endpoint_on_open_vars)
                    result = True
                except Exception: # pylint: disable=broad-except
                    result = False
            elif isinstance(self.__ws_endpoint_on_open_vars, list):
                try:
                    for __send_data in self.__ws_endpoint_on_open_vars:
                        ##print('OV: ' + str(__send_data))
                        ws.send(__send_data)
                        time.sleep(0.14)
                    result = True
                except Exception: # pylint: disable=broad-except
                    result = False

        else:
            result = True

        return result

    def __manage_websocket_reconnect(self, ws):
        """
        websocket.WebSocketApp on_reconnect function.
        ========================================
            Some exchange endpoints require send specific data when websocket is opened 
                :param self: Ccxw instance.
                :param ws: websocket.WebSocketApp instance.

                :return None:
        """
        result = self.__manage_websocket_open(ws)
        return result

    def __manage_websocket_close(self, ws, close_status_code, close_msg):
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

        def force_close():
            try:
                if ws.sock:  # Si el socket sigue abierto después del timeout
                    ws.sock.close()
                    print(f'Cerrando socket {self.__exchange}')
            except Exception: # pylint: disable=broad-except
                self.__auxiliary_class.reset_ws_temp_data()
                print(f'ERROR: Cerrando socket {self.__exchange}')

        if close_status_code is None and close_msg is None:
            self.__ws_ended = True
        else:
            self.__auxiliary_class.reset_ws_temp_data()

        timer = threading.Timer(40, force_close)
        timer.start()
        timer.join(45)

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

    def __manage_websocket_message(self, ws, message_in):
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

        #print(str(message_in))

        try:
            __managed_data = self.__auxiliary_class.manage_websocket_message(ws, message_in)

            #pprint.pprint(__managed_data, sort_dicts=False)

            if __managed_data is not None and isinstance(__managed_data,dict)\
                and 'data' in __managed_data and 'max_proc_time_ms' in __managed_data:

                __endpoint = ''
                __symbol = ''
                __interval = 'none'

                if __managed_data['data'] is not None:
                    if isinstance(__managed_data['data'], list):
                        if len(__managed_data['data']) > 0:
                            __endpoint = __managed_data['data'][0]['endpoint']
                            __symbol = __managed_data['data'][0]['symbol']

                            if 'interval' in __managed_data['data'][0]\
                                and __managed_data['data'][0]['interval'] is not None:
                                __interval = __managed_data['data'][0]['interval']

                    elif isinstance(__managed_data['data'], dict):
                        __endpoint = __managed_data['data']['endpoint']
                        __symbol = __managed_data['data']['symbol']

                        if 'interval' in __managed_data['data']\
                            and __managed_data['data']['interval'] is not None:
                            __interval = __managed_data['data']['interval']

                #print('I: ' + str(__endpoint) + ', ' + str(__symbol) + ', ' + str(__symbol))

                if len(__endpoint) > 0 and len(__symbol) > 0:
                    __index_key_sel = self.__auxiliary_class.get_stream_index(__endpoint,\
                                                                            __symbol,\
                                                                            __interval)

                    __ws_temp_data = __managed_data
                    __ws_temp_data['min_proc_time_ms'] = self.min_proc_time_ms
                    __ws_temp_data['max_proc_time_ms'] = self.max_proc_time_ms
                    __message_out = json.dumps(__ws_temp_data)
                    __message_out = gzip.compress(bytes(__message_out,'utf-8'), compresslevel=9)
                    __message_base64 = base64.b64encode(__message_out).decode("ascii")
                    __sql_update = (
                        f'UPDATE {self.__table_name} SET value_data = ? WHERE key_data = ?;'
                    )

                    with self.__conn_db_lock:
                        self.__cursor_db = self.__conn_db.cursor()
                        self.__cursor_db.execute(__sql_update,\
                                                (str(__message_base64),\
                                                str(self.__key_sel[__index_key_sel])))
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

    def get_current_data(self, endpoint, symbol, interval='none'):
        """
        Ccxw get_current_data function.
        ===============================
            This function get de data from temporal database decode from base64,
            decompress and convert json data in dict and then return this data
                :param self: Ccxw instance.
                :param endpoint: str.
                :param symbol: str.
                :param interval: str.

                :return: dict with last data.
        """
        result = None

        __index_key_sel = self.__auxiliary_class.get_stream_index(endpoint, symbol, interval)

        __sql_select = f'SELECT value_data FROM "{self.__table_name}" WHERE key_data = ?;'

        try:
            with self.__conn_db_lock:
                __local_cursor_db = self.__conn_db.cursor()
                __local_cursor_db.execute(__sql_select, (str(self.__key_sel[__index_key_sel]),))
                __current_data = __local_cursor_db.fetchone()

            if __current_data is not None\
                and isinstance(__current_data,(list, tuple))\
                and len(__current_data) > 0 and __current_data[0] is not None\
                and isinstance(__current_data[0],str):
                result = (
                    json.loads(gzip.decompress(base64.b64decode(__current_data[0])).decode('utf-8'))
                )

                if result is not None and isinstance(result, dict) and 'data' in result\
                    and result['data'] is not None:
                    __data = result['data']
                    if isinstance(__data, dict):
                        if 'symbol' in __data:
                            __data['symbol'] = self.__auxiliary_class\
                                .get_unified_symbol_from_symbol(__data['symbol'])

                        if 'interval' in __data:
                            __data['interval'] = self.__auxiliary_class\
                                .get_unified_interval_from_interval(__data['interval'])

                    elif isinstance(__data, list):
                        for res in __data:
                            if isinstance(res, dict):
                                if 'symbol' in res:
                                    res['symbol'] = self.__auxiliary_class\
                                        .get_unified_symbol_from_symbol(res['symbol'])

                                if 'interval' in res:
                                    res['interval'] = self.__auxiliary_class\
                                        .get_unified_interval_from_interval(res['interval'])

                    result['data'] = __data

        except Exception as exc: # pylint: disable=broad-except
            print(str(exc))

        return result

    def get_sqlite_memory_used(self):
        """
        Ccxw get_sqlite_memory_used function.
        =====================================
            This method return a estimated bytes used for sqlite temporary database
                :param self: Ccxw instance.

                :return: int.
        """

        result = 0

        with self.__conn_db_lock:
            try:
                __sql_to_exec = 'select page_size * page_count'
                __sql_to_exec += ' from pragma_page_count(), pragma_page_size();'
                cursor = self.__conn_db.cursor()
                cursor.execute(__sql_to_exec)
                result = int(cursor.fetchone()[0])
            except Exception: # pylint: disable=broad-except
                result = 0

        return result

    def get_sqlite_memory_used_human_readable(self):
        """
        Ccxw get_sqlite_memory_used_human_readable function.
        ====================================================
            This method return a estimated bytes used for sqlite temporary database\
                ( In human readable format)
                :param self: Ccxw instance.

                :return: str.
        """

        result = ''
        __db_size = self.get_sqlite_memory_used()
        result = humanize.naturalsize(__db_size)

        return result

    def is_connections_ok(self):
        """
        Ccxw is_connections_ok function.
        ================================
                :param self: Ccxw instance.

                :return: bool.
        """
        result = False
        __get_limit_times = 5
        __time_interval = 60

        try:
            if self.__ws_streams is not None and isinstance(self.__ws_streams, list):
                result = True
                __current_time = time.time()

                for __stream in self.__ws_streams:
                    __last_get_time = 0
                    __endpoint, __symbol, __interval = (
                        __stream['endpoint'], __stream['symbol'], 'none'
                    )
                    if 'interval' in __stream:
                        __interval = __stream['interval']

                    __data = self.get_current_data(__endpoint, __symbol, __interval)
                    __cmp = False

                    if __endpoint == 'order_book':
                        if __data is not None and isinstance(__data, dict):
                            __last_get_time = float(__data['data']['timestamp'])
                            __cmp = (__current_time - __last_get_time)\
                                <= (__get_limit_times * __time_interval)
                            result = __cmp and result
                        else:
                            __cmp = (__current_time - self.__start_time)\
                                > (2 *__get_limit_times * __time_interval)
                            if __cmp:
                                result = False

                    elif __endpoint == 'kline':
                        __time_interval = Ccxw.get_delta_time_from_interval(__interval)
                        if __data is not None and isinstance(__data, dict):
                            if len(__data['data']) > 0:
                                __last_get_time = float(__data['data'][-1]['close_time']) / 1000
                            __cmp = (__current_time - __last_get_time)\
                                <= (__get_limit_times * __time_interval)
                            result = __cmp and result
                        else:
                            __cmp = (__current_time - self.__start_time)\
                                > (2 *__get_limit_times * __time_interval)
                            if __cmp:
                                result = False
                    elif __endpoint == 'trades':
                        if __data is not None and isinstance(__data, dict):
                            if len(__data['data']) > 0:
                                __last_get_time = float(__data['data'][-1]['trade_time']) / 1000
                            __cmp = (__current_time - __last_get_time)\
                                <= (9 * __get_limit_times * __time_interval)
                            result = __cmp and result
                        else:
                            __cmp = (__current_time - self.__start_time)\
                                > (9 *__get_limit_times * __time_interval)
                            if __cmp:
                                result = False
                    elif __endpoint == 'ticker':
                        if __data is not None and isinstance(__data, dict):

                            __last_get_time = float(__data['data']['event_time']) / 1000
                            __cmp = (__current_time - __last_get_time)\
                                <= (9 * __get_limit_times * __time_interval)
                            result = __cmp and result
                        else:
                            __cmp = (__current_time - self.__start_time)\
                                > (9 *__get_limit_times * __time_interval)
                            if __cmp:
                                result = False

        except Exception: # pylint: disable=broad-except
            result = False

        return result

    @classmethod
    def get_delta_time_from_interval(cls, interval):
        """
        get_delta_time_from_interval
        ============================
        """
        result = 60

        __intervals_map = {
            '1m': 60,
            '3m': 180,
            '5m': 300,
            '15m': 900,
            '30m': 1800,
            '1h': 3600,
            '2h': 7200,
            '4h': 14400,
            '6h': 21600,
            '12h': 43200,
            '1d': 86400,
            '1w': 604800,
            '1mo': 86400 * 30
        }

        if interval in __intervals_map:
            result = __intervals_map[interval]

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
        __suported_exchanges = ['binance', 'bybit', 'kucoin', 'bingx','okx', 'binanceus']

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
