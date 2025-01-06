# pylint: disable=too-many-lines
"""
Ccxw - CryptoCurrency eXchange Websocket Library
Okx auxiliary functions

Author: Ricardo Marcelo Alvarez
Date: 2023-10-31
"""

import os
import json
import time
import datetime
import queue
import math
import random
import pprint # pylint: disable=unused-import
import threading
import websocket
import websocket_server

import ccxw.ccxw_common_functions as ccf
from ccxw.safe_thread_vars import DictSafeThread
import ccxw

class OkxCcxwAuxClass():
    """
    Ccxw - CryptoCurrency eXchange Websocket Library OkxCcxwAuxClass
    ================================================================
        This class contains helper functions for the Ccxw class
    """

    def __init__(self,\
                 streams: list[dict],\
                 trading_type: str='SPOT',\
                 testmode: bool=False,\
                 result_max_len: int=5,\
                 data_max_len: int=2500,\
                 debug: bool=False):
        """
        OkxCcxwAuxClass constructor
        ===========================
            :param self: OkxCcxwAuxClass instance.
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
            :param debug: bool Verbose output.

            :return: Return a new instance of the Class OkxCcxwAuxClass.
        """

        __exchange_limit_streams = 480

        self.__exchange = os.path.basename(__file__)[:-3]
        self.__ws_streams = streams
        self.__testmode = testmode

        self.__ws_server = None
        self.__ws_server_url = '127.0.0.1'
        self.__ws_server_port_min = 10000
        self.__ws_server_port_max = 50000

        self.__ws_server_port = self.__ws_server_port_min

        self.__local_ws_url = 'ws://' + self.__ws_server_url + ':' + str(self.__ws_server_port)

        self.__exchange_info_cache = {}
        self.__exchange_info_cache['data'] = None
        self.__exchange_info_cache['last_get_time'] = 0

        self.__debug = debug
        self.__trading_type = trading_type
        self.__data_max_len = data_max_len

        self.__data_max_len = min(self.__data_max_len, 2500)
        self.__data_max_len = max(self.__data_max_len, 1)

        self.__result_max_len = result_max_len

        self.__result_max_len = min(self.__result_max_len, self.__data_max_len)
        self.__result_max_len = max(self.__result_max_len, 1)

        self.__ws_url_api = None
        self.__ws_url_test = None
        self.__ws_url = None
        self.__url_api = None
        self.__url_test = None
        self.__ws_endpoint_url = None
        self.__ws_endpoint_on_open_vars = None
        self.__ws_endpoint_on_close_vars = None

        self.__ws_public = None
        self.__ws_endpoint_url_public = None
        self.__ws_endpoint_on_open_vars_public = None
        self.__ws_endpoint_on_close_vars_public = None
        self.__ws_bussiness = None
        self.__ws_endpoint_url_bussiness = None
        self.__ws_endpoint_on_open_vars_bussiness = None
        self.__ws_endpoint_on_close_vars_bussiness = None

        self.__ws_ping_interval = 0
        self.__ws_ping_timeout = None

        self.__ws_temp_data = DictSafeThread()
        self.__lock = threading.Lock()

        self.__thread_public = None
        self.__thread_bussiness = None
        self.__thread_ping = None
        self.__last_pong_time_public = 0
        self.__last_pong_time_bussiness = 0

        websocket.enableTrace(self.__debug)

        self.__is_stopped = False
        self.__lock_stopped = threading.Lock()

        if not self.__check_streams_struct(streams):
            raise ValueError('The streams struct is not valid' + str(streams))

        if len(streams) > __exchange_limit_streams:
            raise ValueError('The exchange ' + str(self.__exchange)\
                             + ' not alowed more than ' + str(__exchange_limit_streams)\
                             + ' of streams.')

    def reset_ws_temp_data(self):
        """
        reset_ws_temp_data
        ==================
        """
        for key_d in self.__ws_temp_data:
            self.__ws_temp_data[key_d] = None

    def __del__(self):

        with self.__lock_stopped:
            if not self.__is_stopped:
                self.stop()

    def start(self):
        """
        start
        =====

            :param self: OkxAuxClass instance.
        """

        with self.__lock_stopped:
            self.__is_stopped = False

        self.__start_ws_server()
        time.sleep(1)
        self.__start_ws_clients()

    def stop(self):
        """
        stop
        ====

            :param self: OkxAuxClass instance.
        """
        with self.__lock_stopped:
            self.__is_stopped = True

        self.__stop_ws_clients()

        time.sleep(5)

        if self.__ws_server is not None:
            self.__ws_server.deny_new_connections()
            self.__ws_server.disconnect_clients_gracefully()
            time.sleep(2)
            self.__ws_server.shutdown_gracefully()
            time.sleep(2)
            self.__ws_server.disconnect_clients_abruptly()
            time.sleep(3)
            self.__ws_server.server_close()
            # self.__ws_server.shutdown_abruptly()
            # self.__ws_server = None

    def __check_streams_struct(self, streams):
        """
        __check_streams_struct
        ======================
            This function check streams struct
                :param self: This class instance.
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

                :return bool: True if streams struct is correct otherwise False
        """
        result = False

        if streams is not None and isinstance(streams, list) and len(streams) > 0:
            result = True
            for stream in streams:
                result = result and stream is not None\
                    and isinstance(stream, dict)\
                    and 'endpoint' in stream\
                    and stream['endpoint'] is not None\
                    and isinstance(stream['endpoint'], str)\
                    and stream['endpoint'] in ccxw.Ccxw.get_supported_endpoints()\
                    and 'symbol' in stream\
                    and stream['symbol'] is not None\
                    and isinstance(stream['symbol'], str)\
                    and self.if_symbol_supported(stream['symbol'])

                if result and stream['endpoint'] == 'kline':
                    result = 'interval' in stream\
                        and stream['interval'] is not None\
                        and isinstance(stream['interval'], str)\
                        and stream['interval'] in\
                            ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h',\
                             '6h', '8h', '12h', '1d', '3d', '1w', '1mo']

        return result

    def __set_ws_server(self):
        result = False

        __host = '127.' + str(random.randint(0, 254)) + '.' + str(random.randint(0, 254))\
            + '.' + str(random.randint(1, 254))
        __port = random.randint(self.__ws_server_port_min, self.__ws_server_port_max)

        while not ccf.is_port_free(__port, __host):
            __host = '127.' + str(random.randint(0, 254)) + '.' + str(random.randint(0, 254))\
                + '.' + str(random.randint(1, 254))
            __port = random.randint(self.__ws_server_port_min, self.__ws_server_port_max)

        self.__ws_server_url = __host
        self.__ws_server_port = __port

        result = True

        return result

    def __start_ws_server(self):
        result = False
        __attemps = 0
        __attemps_limit = 900

        while not ccf.is_port_free(self.__ws_server_port, self.__ws_server_url)\
            and __attemps <= __attemps_limit:
            __attemps = __attemps + 1
            time.sleep(1)

        if ccf.is_port_free(self.__ws_server_port, self.__ws_server_url):
            with self.__lock:
                self.__ws_server = websocket_server.WebsocketServer(host=self.__ws_server_url,\
                                                                    port=self.__ws_server_port)

                self.__ws_server.run_forever(threaded=True)
                result = True

        return result

    def __manage_websocket_open(self, ws):
        result = False

        if hasattr(ws, 'on_open_vars') and ws.on_open_vars is not None:
            try:
                ws.send(ws.on_open_vars)
                result = True
            except Exception: # pylint: disable=broad-except
                result = False

        return result

    def __manage_websocket_reconnect(self, ws):
        result = self.__manage_websocket_open(ws)
        return result

    def __manage_websocket_close(self, ws, close_status_code, close_msg): # pylint: disable=unused-argument
        result = False

        def force_close():
            try:
                if ws.sock:  # Si el socket sigue abierto después del timeout
                    ws.sock.close()
                    print('Cerrando socket okx')
            except Exception: # pylint: disable=broad-except
                pass

        if hasattr(ws, 'on_close_vars') and ws.on_close_vars is not None:
            try:
                ws.send(ws.on_close_vars)
                result = True
            except Exception: # pylint: disable=broad-except
                result = False

        timer = threading.Timer(40, force_close)
        timer.start()
        timer.join(45)

        return result

    def __manage_websocket_message_local_public(self, ws, message): # pylint: disable=unused-argument

        if self.__ws_server is not None:
            if message is not None and ccf.is_json(message):
                with self.__lock:
                    # __message_out = json.loads(message)
                    # pprint.pprint(__message_out, sort_dicts=False)
                    self.__ws_server.send_message_to_all(message)

            elif message is not None and isinstance(message, str):
                if message.lower() == 'pong':
                    with self.__lock_stopped:
                        self.__last_pong_time_public = time.time()

    def __manage_websocket_message_local_bussiness(self, ws, message): # pylint: disable=unused-argument

        if self.__ws_server is not None:
            if message is not None and ccf.is_json(message):
                with self.__lock:
                    # __message_out = json.loads(message)
                    # pprint.pprint(__message_out, sort_dicts=False)
                    self.__ws_server.send_message_to_all(message)

            elif message is not None and isinstance(message, str):
                if message.lower() == 'pong':
                    with self.__lock_stopped:
                        self.__last_pong_time_bussiness = time.time()

    def __start_ws_clients(self):
        with self.__lock_stopped:
            self.__is_stopped = False

        self.__thread_public = threading.Thread(target=self.__start_ws_client_public)
        self.__thread_public.start()

        self.__thread_bussiness = threading.Thread(target=self.__start_ws_client_bussiness)
        self.__thread_bussiness.start()

        self.__thread_ping = threading.Thread(target=self.__start_ping_thread)
        self.__thread_ping.start()

    def __stop_ws_clients(self):
        with self.__lock_stopped:
            self.__is_stopped = True

        if self.__thread_ping is not None\
            and threading.current_thread() is not self.__thread_ping\
            and self.__thread_ping.is_alive():
            try:
                self.__thread_ping.join(45)
            except Exception: # pylint: disable=broad-except
                pass

        self.__ws_public.close()
        self.__ws_bussiness.close()

        if self.__thread_public is not None\
            and threading.current_thread() is not self.__thread_public\
            and self.__thread_public.is_alive():

            try:
                self.__thread_public.join(45)
            except Exception: # pylint: disable=broad-except
                pass

        if self.__thread_bussiness is not None\
            and threading.current_thread() is not self.__thread_bussiness\
            and self.__thread_bussiness.is_alive():

            try:
                self.__thread_bussiness.join(45)
            except Exception: # pylint: disable=broad-except
                pass

    def __start_ping_thread(self):
        result = False

        __run_ping = True

        __pong_time_limit = 30

        __last_pong_public = 0
        __last_pong_bussiness = 0

        __to_reconnect_public = False # Not used for now
        __to_reconnect_bussiness = False # Not used for now

        time.sleep(9)

        while __run_ping:
            __last_pong_public = 0
            __last_pong_bussiness = 0

            with self.__lock_stopped:
                __run_ping = not self.__is_stopped

            if __run_ping:
                __current_time_public = time.time()
                __current_time_bussiness = time.time()
                try:
                    if self.__ws_public is not None:
                        self.__ws_public.send('ping')
                        __current_time_public = time.time()
                    if self.__ws_bussiness is not None:
                        self.__ws_bussiness.send('ping')
                        __current_time_bussiness = time.time()
                except Exception: # pylint: disable=broad-except
                    pass

                time.sleep(25)
                with self.__lock_stopped:
                    __last_pong_public = self.__last_pong_time_public
                    __last_pong_bussiness = self.__last_pong_time_bussiness

                if (__last_pong_public - __current_time_public) > __pong_time_limit:
                    __to_reconnect_public = True

                if (__last_pong_bussiness - __current_time_bussiness) > __pong_time_limit:
                    __to_reconnect_bussiness = True

        return result

    def __start_ws_client_public(self):
        result = False
        __socket = self.__ws_url + self.__ws_endpoint_url_public

        try:
            self.__ws_public = (
                websocket.WebSocketApp(__socket,\
                                       on_message=self.__manage_websocket_message_local_public,\
                                       on_open=self.__manage_websocket_open,\
                                       on_reconnect=self.__manage_websocket_reconnect,\
                                       on_close=self.__manage_websocket_close)
            )
            self.__ws_public.on_open_vars = self.__ws_endpoint_on_open_vars_public
            self.__ws_public.on_close_vars = self.__ws_endpoint_on_close_vars_public
            result = True

            __ws_temp = (
                self.__ws_public.run_forever(ping_interval=self.__ws_ping_interval,\
                                             ping_timeout=self.__ws_ping_timeout,\
                                             reconnect=50)
                )

        except Exception as exc: # pylint: disable=broad-except
            result = False
            print('On create websocket exception: ' + str(exc))

        return result

    def __start_ws_client_bussiness(self):
        result = False
        __socket = self.__ws_url + self.__ws_endpoint_url_bussiness

        try:
            self.__ws_bussiness = (
                websocket.WebSocketApp(__socket,\
                                       on_message=self.__manage_websocket_message_local_bussiness,\
                                       on_open=self.__manage_websocket_open,\
                                       on_reconnect=self.__manage_websocket_reconnect,\
                                       on_close=self.__manage_websocket_close)
            )
            self.__ws_bussiness.on_open_vars = self.__ws_endpoint_on_open_vars_bussiness
            self.__ws_bussiness.on_close_vars = self.__ws_endpoint_on_close_vars_bussiness
            result = True

            __ws_temp = (
                self.__ws_bussiness.run_forever(ping_interval=self.__ws_ping_interval,\
                                                ping_timeout=self.__ws_ping_timeout,\
                                                reconnect=50)
                )

        except Exception as exc: # pylint: disable=broad-except
            result = False
            print('On create websocket exception: ' + str(exc))

        return result

    def get_websocket_url(self):
        """
        get_websocket_url
        =================
            This function set and return websocket URL.

                :return str: Return websocket URL.
        """

        result = None

        if self.__set_ws_server():

            self.__local_ws_url = 'ws://' + self.__ws_server_url + ':' + str(self.__ws_server_port)

            if self.__trading_type == 'SPOT':
                self.__ws_url_api = 'wss://ws.okx.com:8443/ws/v5'
                self.__ws_url_test = 'wss://wspap.okx.com:8443/ws/v5'

            self.__ws_url = self.__ws_url_api
            if self.__testmode:
                self.__ws_url = self.__ws_url_test

            result = self.__local_ws_url

        return result

    def get_api_url(self):
        """
        get_api_url
        ===========
            This function set and return API URL.

                :return str: Return API URL.
        """

        result = None

        if self.__trading_type == 'SPOT':
            self.__url_api = 'https://www.okx.com'
            self.__url_test = 'https://www.okx.com'

        result = self.__url_api
        if self.__testmode:
            result = self.__url_test

        return result

    def get_exchange_info(self):
        """
        get_exchange_info
        =================
            This function get exchange info.

                :return dict: Return exchange info.
        """

        result = None

        max_last_get_time = 7200

        current_time = int(time.time())

        if (current_time - self.__exchange_info_cache['last_get_time']) >= max_last_get_time:
            self.__exchange_info_cache['data'] = None

        if self.__exchange_info_cache['data'] is None:

            __l_url_api = self.get_api_url()

            __l_endpoint = '/api/v5/public/instruments?instType=' + self.__trading_type.upper()

            __l_url_point = __l_url_api + __l_endpoint

            headers = {}
            headers['user-agent'] = 'ccxw class'
            headers['accept'] = '*/*'

            __data = ccf.file_get_contents_url(__l_url_point,'r',None,headers)

            if __data is not None and ccf.is_json(__data):
                result = json.loads(__data)
                self.__exchange_info_cache['data'] = result
                self.__exchange_info_cache['last_get_time'] = current_time

        else:
            result = self.__exchange_info_cache['data']

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
        __main_data = self.get_exchange_info()

        if __main_data is not None and isinstance(__main_data,dict)\
            and 'data' in __main_data and isinstance(__main_data['data'],list):

            result = []
            for symbol_data in __main_data['data']:
                if symbol_data is not None and isinstance(symbol_data,dict)\
                    and 'baseCcy' in symbol_data and 'quoteCcy' in symbol_data:
                    if isinstance(symbol_data['baseCcy'],str)\
                        and isinstance(symbol_data['quoteCcy'],str):
                        result.append(\
                            symbol_data['baseCcy'].upper() + '/'\
                            + symbol_data['quoteCcy'].upper())

            if sort_list:
                result.sort()

        return result

    def get_unified_symbol_from_symbol(self, symbol):
        """
        get_unified_symbol_from_symbol
        ==============================
            This function get unified symbol from symbol.
                :param self: This class instance.
                :param symbol: str.
                :return str: Return unified symbol.
        """
        result = symbol

        full_list_symbols = self.get_exchange_full_list_symbols(False)

        for symbol_rpl in full_list_symbols:
            if symbol.replace('/', '').replace('-', '').lower()\
                == symbol_rpl.replace('/', '').replace('-', '').lower():
                result = symbol_rpl
                break

        return result

    def __get_interval_from_unified_interval(self, interval):

        result = interval

        if interval is not None:
            result = str(interval)

            if 'm' in result:
                pass
            elif 'h' in result or 'H' in result:
                result = result.upper() + 'utc'
            elif 'd' in result or 'D' in result:
                result = result.upper() + 'utc'
            elif 'w' in result or 'W' in result:
                result = result.upper() + 'utc'
            elif 'mo' in result:
                result = result.replace('mo', 'M').upper() + 'utc'

            result = str(result)

        return result

    def get_unified_interval_from_interval(self, interval):
        """
        get_unified_interval_from_interval
        ==================================
            This function get unified interval from interval.
                :param self: This class instance.
                :param interval: str.
                :return str: Return unified interval.
        """

        result = interval

        if result is not None:
            result = result.replace('utc', '')

            if 'm' in result:
                pass
            elif 'H' in result:
                result = result.lower()
            elif 'D' in result:
                result = result.lower()
            elif 'W' in result:
                result = result.lower()
            elif 'M' in result:
                result = result.replace('M', 'mo').lower()

            result = str(result)

        return result

    def if_symbol_supported(self, symbol):
        """
        if_symbol_supported
        ===================
            This function check if symbol is supported by the exchange.

                :return bool: Return True if supported 
        """

        result = False

        __data = self.get_exchange_full_list_symbols()

        if isinstance(__data,list) and symbol in __data:
            result = True

        return result

    def get_stream_index(self, endpoint, symbol, interval: str='none'):
        """
        get_stream_index
        ================
            This function return a stream index for use in dict.
                :param self: This class instance.
                :param endpoint: str.
                :param symbol: str unified symbol.
                :param interval: str.
                :return str: Return stream index
        """
        result = 'stream'

        if interval is None:
            interval = 'none'

        result = result + '_' + endpoint + '_' + symbol + '_' + interval

        result = result.replace('/','').lower()

        return result

    def get_websocket_endpoint_path(self):
        """
        get_websocket_endpoint_path
        ===========================
            This function set endpoint URL and on_open vars and on_close vars.

                :return dict: Return dict with endpoint, open_vars and close_vars
        """

        result = None

        __send_data_vars = None

        self.__ws_endpoint_url = ''

        self.__ws_endpoint_url_public = '/public'
        self.__ws_endpoint_on_open_vars_public = {}
        self.__ws_endpoint_on_close_vars_public = {}
        self.__ws_endpoint_on_open_vars_public['op'] = 'subscribe'
        self.__ws_endpoint_on_open_vars_public['args'] = None
        self.__ws_endpoint_on_close_vars_public['op'] = 'unsubscribe'
        self.__ws_endpoint_on_close_vars_public['args'] = None

        self.__ws_endpoint_url_bussiness = '/business'
        self.__ws_endpoint_on_open_vars_bussiness = {}
        self.__ws_endpoint_on_close_vars_bussiness = {}
        self.__ws_endpoint_on_open_vars_bussiness['op'] = 'subscribe'
        self.__ws_endpoint_on_open_vars_bussiness['args'] = None
        self.__ws_endpoint_on_close_vars_bussiness['op'] = 'unsubscribe'
        self.__ws_endpoint_on_close_vars_bussiness['args'] = None

        __channel_args_public = None
        __channel_args_public = []

        __channel_args_bussiness = None
        __channel_args_bussiness = []

        for stream in self.__ws_streams:
            interval = 'none'

            if stream['endpoint'] == 'order_book':
                __channel_vars = None
                __channel_vars = {}
                __channel_vars['channel'] = 'books'
                __channel_vars['instId'] = stream['symbol'].replace("/","-").upper()
                __channel_args_public.append(__channel_vars)

            elif stream['endpoint'] == 'kline':
                interval = stream['interval']

                __channel_out = 'candle' + self.__get_interval_from_unified_interval(interval)
                __channel_vars = None
                __channel_vars = {}
                __channel_vars['channel'] = __channel_out
                __channel_vars['instId'] = stream['symbol'].replace("/","-").upper()
                __channel_args_bussiness.append(__channel_vars)

            elif stream['endpoint'] == 'trades':
                __channel_vars = None
                __channel_vars = {}
                __channel_vars['channel'] = 'trades'
                __channel_vars['instId'] = stream['symbol'].replace("/","-").upper()
                __channel_args_public.append(__channel_vars)


            elif stream['endpoint'] == 'ticker':
                __channel_vars = None
                __channel_vars = {}
                __channel_vars['channel'] = 'tickers'
                __channel_vars['instId'] = stream['symbol'].replace("/","-").upper()
                __channel_args_public.append(__channel_vars)

            __stream_index = self.get_stream_index(stream['endpoint'],\
                                                     stream['symbol'],\
                                                     interval=interval)
            self.__ws_temp_data[__stream_index] = None

        if self.__testmode:
            self.__ws_endpoint_url_public = self.__ws_endpoint_url_public + '?brokerId=9999'
            self.__ws_endpoint_url_bussiness = self.__ws_endpoint_url_bussiness + '?brokerId=9999'

        self.__ws_endpoint_on_open_vars = None
        self.__ws_endpoint_on_close_vars = None

        self.__ws_endpoint_on_open_vars_public['args'] = __channel_args_public
        self.__ws_endpoint_on_close_vars_public['args'] = __channel_args_public

        self.__ws_endpoint_on_open_vars_public = (
            json.dumps(self.__ws_endpoint_on_open_vars_public)
        )
        self.__ws_endpoint_on_close_vars_public = (
            json.dumps(self.__ws_endpoint_on_close_vars_public)
        )

        self.__ws_endpoint_on_open_vars_bussiness['args'] = __channel_args_bussiness
        self.__ws_endpoint_on_close_vars_bussiness['args'] = __channel_args_bussiness

        self.__ws_endpoint_on_open_vars_bussiness = (
            json.dumps(self.__ws_endpoint_on_open_vars_bussiness)
        )
        self.__ws_endpoint_on_close_vars_bussiness = (
            json.dumps(self.__ws_endpoint_on_close_vars_bussiness)
        )

        result = {}
        result['ws_endpoint_url'] = self.__ws_endpoint_url
        result['ws_endpoint_on_open_vars'] = self.__ws_endpoint_on_open_vars
        result['ws_endpoint_on_close_vars'] = self.__ws_endpoint_on_close_vars

        return result

    def __init_order_book_data(self, temp_data, __stream_index):

        result = False

        if temp_data is not None and isinstance(temp_data,dict) and 'data' in temp_data\
            and isinstance(temp_data['data'],list) and len(temp_data['data']) > 0:
            if isinstance(temp_data['data'][0],dict):
                if 'bids' in temp_data['data'][0] and 'asks' in temp_data['data'][0]\
                    and isinstance(temp_data['data'][0]['bids'],list)\
                    and isinstance(temp_data['data'][0]['asks'],list):
                    __bids = temp_data['data'][0]['bids']
                    __asks = temp_data['data'][0]['asks']

                    __data_out = temp_data
                    __data_out['data'][0]['bids'] = []
                    __data_out['data'][0]['asks'] = []

                    for __bid in __bids:
                        __data_out['data'][0]['bids'].append([__bid[0],__bid[1]])

                    for __ask in __asks:
                        __data_out['data'][0]['asks'].append([__ask[0],__ask[1]])

                    self.__ws_temp_data[__stream_index] = __data_out

                    result = True

        return result

    def manage_websocket_message_order_book(self, data):
        """
        manage_websocket_message_order_book
        ===================================
            This function manage websocket message and normalize result
            data for order_book endpoint.

                :param data: dict.
                :return dict: Return dict with normalized data.
        """
        result = None

        __temp_data = data
        __proc_data = False
        __diff_update_id = 0
        __data_type = 'snapshot'


        if __temp_data is not None and isinstance(__temp_data, dict)\
            and 'action' in __temp_data:

            __symbol = self.get_unified_symbol_from_symbol(__temp_data['arg']['instId'])
            __stream_index = self.get_stream_index('order_book', __symbol)

            if __temp_data['action'] == 'snapshot':
                if self.__init_order_book_data(__temp_data, __stream_index):
                    __proc_data = True
            elif __temp_data['action'] == 'update'\
                and self.__ws_temp_data[__stream_index] is not None\
                and isinstance(self.__ws_temp_data[__stream_index], dict):
                __diff_update_id = (
                    __temp_data['data'][0]['seqId']\
                        - self.__ws_temp_data[__stream_index]['data'][0]['seqId']
                )
                if self.__manage_websocket_diff_data(__temp_data, __stream_index):
                    __data_type = 'update'
                    __proc_data = True

            if __proc_data:
                __message_out = None
                __message_out = {}
                __message_out['endpoint'] = 'order_book'
                __message_out['exchange'] = self.__exchange
                __message_out['symbol'] = __symbol
                __message_out['interval'] = None
                __message_out['last_update_id'] = __temp_data['data'][0]['seqId']
                __message_out['diff_update_id'] = __diff_update_id
                __message_out['bids'] = (
                    self.__ws_temp_data[__stream_index]['data'][0]['bids'][:self.__result_max_len]
                )
                __message_out['asks'] = (
                    self.__ws_temp_data[__stream_index]['data'][0]['asks'][:self.__result_max_len]
                )
                __message_out['type'] = __data_type
                __current_datetime = datetime.datetime.now(datetime.timezone.utc)
                __current_timestamp = __current_datetime.strftime("%s.%f")
                __current_datetime = __current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
                __message_out['timestamp'] = __current_timestamp
                __message_out['datetime'] = __current_datetime

                result = __message_out

        return result

    def manage_websocket_message_kline(self,data):
        """
        manage_websocket_message_kline
        ==============================
            This function manage websocket message and normalize result
            data for kline endpoint.

                :param data: dict.
                :return dict: Return dict with normalized data.
        """
        result = None

        __temp_data = data
        __proc_data = False

        __symbol = self.get_unified_symbol_from_symbol(__temp_data['arg']['instId'])
        __interval = self.get_unified_interval_from_interval(__temp_data['arg']['channel']\
                                                             .replace('candle', ''))

        __stream_index = self.get_stream_index('kline', __symbol, __interval)

        __delta_time = 60000

        if __interval[-1] == 'm':
            __delta_mult = int(__interval.replace('m',''))
            __delta_time = 60000 * __delta_mult

        elif __interval[-1] == 'h' or __interval[-1] == 'H':
            __delta_mult = int(__interval.replace('h','').replace('H',''))
            __delta_time = 60000 * __delta_mult * 60

        elif __interval[-1] == 'd' or __interval[-1] == 'D':
            __delta_mult = int(__interval.replace('d','').replace('D',''))
            __delta_time = 60000 * __delta_mult * 60 * 24

        elif __interval[-1] == 'w' or __interval[-1] == 'W':
            __delta_mult = int(__interval.replace('w','').replace('W',''))
            __delta_time = 60000 * __delta_mult * 60 * 24

        __delta_time = __delta_time - 1

        if __temp_data is not None and isinstance(__temp_data, dict)\
            and 'data' in __temp_data and isinstance(__temp_data['data'],list):
            if len(__temp_data['data']) > 0\
                and isinstance(__temp_data['data'][0],list)\
                and len(__temp_data['data'][0]) >= 9:

                if self.__ws_temp_data[__stream_index] is None\
                    or not isinstance(self.__ws_temp_data[__stream_index], dict):
                    self.__ws_temp_data[__stream_index] = {}

                for i in range(0,len(__temp_data['data'])):
                    __is_confirmed = False
                    if int(__temp_data['data'][i][8]) == 1:
                        __is_confirmed = True

                    __message_add = None
                    __message_add = {}
                    __message_add['endpoint'] = 'kline'
                    __message_add['exchange'] = self.__exchange
                    __message_add['symbol'] = __symbol
                    __message_add['interval'] = __interval
                    __message_add['last_update_id'] = int(__temp_data['data'][i][0])
                    __message_add['open_time'] = int(__temp_data['data'][i][0])
                    __message_add['close_time'] = (
                        int(__temp_data['data'][i][0]) + __delta_time
                    )
                    __message_add['open_time_date'] = (
                        time.strftime("%Y-%m-%d %H:%M:%S",\
                                        time.gmtime(int(round(__message_add['open_time']\
                                                            /1000))))
                    )
                    __message_add['close_time_date'] = (
                        time.strftime("%Y-%m-%d %H:%M:%S",\
                                        time.gmtime(int(round(__message_add['close_time']\
                                                            /1000))))
                    )
                    __message_add['open'] = __temp_data['data'][i][1]
                    __message_add['close'] = __temp_data['data'][i][4]
                    __message_add['hight'] = __temp_data['data'][i][2]
                    __message_add['low'] = __temp_data['data'][i][3]
                    __message_add['volume'] = __temp_data['data'][i][5]
                    __message_add['is_closed'] = __is_confirmed

                    self.__ws_temp_data[__stream_index][int(__message_add['open_time'])] =(
                        __message_add
                    )

                while len(self.__ws_temp_data[__stream_index]) > self.__data_max_len:
                    __first_key = min(list(self.__ws_temp_data[__stream_index].keys()))
                    __nc = self.__ws_temp_data[__stream_index].pop(__first_key,None)

                __message_out = list(self.__ws_temp_data[__stream_index].values())

                result = __message_out[:self.__result_max_len]

        return result

    def manage_websocket_message_trades(self,data):
        """
        manage_websocket_message_trades
        ===============================
            This function manage websocket message and normalize result
            data for trades endpoint.

                :param data: dict.
                :return dict: Return dict with normalized data.
        """
        result = None

        __temp_data = data
        __proc_data = False
        __symbol = self.get_unified_symbol_from_symbol(__temp_data['arg']['instId'])
        __stream_index = self.get_stream_index('trades', __symbol)

        if __temp_data is not None and isinstance(__temp_data,dict)\
            and 'data' in __temp_data\
            and isinstance(__temp_data['data'],list) and len(__temp_data['data']) > 0:

            if self.__ws_temp_data[__stream_index] is None:
                self.__ws_temp_data[__stream_index] = queue.Queue(maxsize=self.__data_max_len)

            for i in range(len(__temp_data['data']) - 1,-1,-1):
                __message_add = None
                __message_add = {}
                __message_add['endpoint'] = 'trades'
                __message_add['exchange'] = self.__exchange
                __message_add['symbol'] = __symbol
                __message_add['interval'] = None
                __message_add['event_time'] = int(round(time.time_ns() / 1000000))
                __message_add['trade_id'] = str(__temp_data['data'][i]['tradeId'])
                __message_add['price'] = str(__temp_data['data'][i]['px'])
                __message_add['quantity'] = str(__temp_data['data'][i]['sz'])
                __message_add['trade_time'] = int(__temp_data['data'][i]['ts'])
                __message_add['trade_time_date'] = (
                    time.strftime("%Y-%m-%d %H:%M:%S",\
                                    time.gmtime(int(round(__message_add['trade_time']\
                                                        /1000)))) + '.'\
                                    + str(round(math.modf(round(\
                                        __message_add['trade_time']\
                                            /1000,3))[0]*1000)).rjust(3,'0')
                )

                __side_of_taker = __temp_data['data'][i]['side'].upper()
                __message_add['side_of_taker'] = __side_of_taker

                if self.__ws_temp_data[__stream_index].full():
                    self.__ws_temp_data[__stream_index].get(True,1)

                self.__ws_temp_data[__stream_index].put(__message_add,True,5)

                __message_out = list(self.__ws_temp_data[__stream_index].queue)
                ##__message_out.reverse()

                result = __message_out[:self.__result_max_len]

        return result

    def manage_websocket_message_ticker(self,data):
        """
        manage_websocket_message_ticker
        ===============================
            This function manage websocket message and normalize result
            data for ticker endpoint.

                :param data: dict.
                :return dict: Return dict with normalized data.
        """
        result = None

        __temp_data = data
        __proc_data = False
        __symbol = self.get_unified_symbol_from_symbol(__temp_data['arg']['instId'])
        __stream_index = self.get_stream_index('ticker', __symbol)

        if __temp_data is not None and isinstance(__temp_data,dict)\
            and 'data' in __temp_data and isinstance(__temp_data['data'],list)\
            and len(__temp_data['data']) > 0:

            self.__ws_temp_data[__stream_index] = __temp_data

            i = 0

            __message_add = None
            __message_add = {}
            __message_add['endpoint'] = 'ticker'
            __message_add['exchange'] = self.__exchange
            __message_add['symbol'] = __symbol
            __message_add['interval'] = None
            __message_add['event_type'] = '24hrTicker'
            __message_add['event_time'] = int(__temp_data['data'][i]['ts'])
            __message_add['event_time_date'] = (
                time.strftime("%Y-%m-%d %H:%M:%S",\
                                time.gmtime(int(round(__message_add['event_time']\
                                                    /1000)))) + '.' +\
                                            str(round(math.modf(round(\
                                                __message_add['event_time']\
                                                    /1000,3))[0]*1000)).rjust(3,'0')
            )
            __message_add['price_change'] = None
            __message_add['price_change_percent'] = None
            __message_add['weighted_average_price'] = None
            __message_add['first_trade_before_the_24hr_rolling_window'] = (
                __temp_data['data'][i]['open24h']
            )
            __message_add['last_price'] = __temp_data['data'][i]['last']
            __message_add['last_quantity'] = __temp_data['data'][i]['lastSz']
            __message_add['best_bid_price'] = __temp_data['data'][i]['bidPx']
            __message_add['best_bid_quantity'] = __temp_data['data'][i]['bidSz']
            __message_add['best_ask_price'] = __temp_data['data'][i]['askPx']
            __message_add['best_ask_quantity'] = __temp_data['data'][i]['askSz']
            __message_add['open_price'] = __temp_data['data'][i]['open24h']
            __message_add['high_price'] = __temp_data['data'][i]['high24h']
            __message_add['low_price'] = __temp_data['data'][i]['low24h']
            __message_add['total_traded_base_asset_volume'] = (
                __temp_data['data'][i]['vol24h']
            )
            __message_add['total_traded_quote_asset_volume'] = (
                __temp_data['data'][i]['volCcy24h']
            )
            __message_add['statistics_open_time'] = None
            __message_add['statistics_open_time_date'] = None
            __message_add['statistics_close_time'] = None
            __message_add['statistics_close_time_date'] = None
            __message_add['total_number_of_trades'] = None

            __message_out = __message_add

            result = __message_out

        return result

    def manage_websocket_message(self,ws,message_in): # pylint: disable=unused-argument
        """
        manage_websocket_message
        ========================
            This function manage websocket message and normalize result data.

                :param ws: WebSocketApp instance.
                :param message_in: str message from websocket.
                
                :return dict: Return dict with normalized data.
        """

        result = None

        try:
            if ccf.is_json(message_in):
                __temp_data = json.loads(message_in)
                __endpoint = 'NONE'

                if __temp_data is not None\
                    and isinstance(__temp_data, dict)\
                    and 'arg' in __temp_data\
                    and __temp_data['arg'] is not None\
                    and isinstance(__temp_data['arg'], dict)\
                    and 'channel' in __temp_data['arg']\
                    and 'instId' in __temp_data['arg']:

                    __tmp_endpoint = __temp_data['arg']['channel']

                    if __tmp_endpoint == 'books':
                        __endpoint = 'order_book'
                    elif __tmp_endpoint.startswith('candle'):
                        __endpoint = 'kline'
                    elif __tmp_endpoint == 'trades':
                        __endpoint = 'trades'
                    elif __tmp_endpoint == 'tickers':
                        __endpoint = 'ticker'

                if __endpoint == 'order_book' and 'action' in __temp_data\
                     and 'data' in __temp_data:

                    __message_out = self.manage_websocket_message_order_book(__temp_data)

                    result = {}
                    result['data'] = __message_out
                    result['min_proc_time_ms'] = 0
                    result['max_proc_time_ms'] = 0

                elif __endpoint == 'kline' and 'data' in __temp_data\
                    and isinstance(__temp_data['data'],list):
                    # pprint.pprint(__temp_data, sort_dicts=False)

                    __message_out = self.manage_websocket_message_kline(__temp_data)

                    result = {}
                    result['data'] = __message_out
                    result['min_proc_time_ms'] = 0
                    result['max_proc_time_ms'] = 0

                elif __endpoint == 'trades' and 'data' in __temp_data:
                    __message_out = self.manage_websocket_message_trades(__temp_data)

                    result = {}
                    result['data'] = __message_out
                    result['min_proc_time_ms'] = 0
                    result['max_proc_time_ms'] = 0

                elif __endpoint == 'ticker' and 'data' in __temp_data:
                    __message_out = self.manage_websocket_message_ticker(__temp_data)

                    result = {}
                    result['data'] = __message_out
                    result['min_proc_time_ms'] = 0
                    result['max_proc_time_ms'] = 0

        except Exception as exc: # pylint: disable=broad-except
            print(str(exc))

        return result

    def __manage_websocket_diff_data(self, diff_data, __stream_index):
        result = False

        current_data = self.__ws_temp_data[__stream_index]

        __comp_0 = isinstance(diff_data,dict) and 'data' in diff_data\
            and isinstance(diff_data['data'],list) and len(diff_data['data']) > 0\
            and isinstance(diff_data['data'][0],dict)
        __comp_1 = 'seqId' in diff_data['data'][0] and 'bids' in diff_data['data'][0]\
            and 'asks' in diff_data['data'][0] and isinstance(diff_data['data'][0]['bids'],list)\
            and isinstance(diff_data['data'][0]['asks'],list)

        if __comp_0 and __comp_1:
            __temp_bids_d = {}
            __temp_asks_d = {}
            __temp_bids_l = []
            __temp_asks_l = []

            __key = 'bids'
            for i in range(0,len(current_data['data'][0][__key])):
                __temp_bids_d[current_data['data'][0][__key][i][0]] = \
                    current_data['data'][0][__key][i][1]

            for i in range(0,len(diff_data['data'][0][__key])):
                if float(diff_data['data'][0][__key][i][1]) == 0:
                    n_t = __temp_bids_d.pop(diff_data['data'][0][__key][i][0],None) # pylint: disable=unused-variable
                else:
                    __temp_bids_d[diff_data['data'][0][__key][i][0]] = \
                        diff_data['data'][0][__key][i][1]

            for i,j in __temp_bids_d.items():
                __temp_bids_l.append([i,j])

            __temp_bids = sorted(__temp_bids_l, key=lambda value: float(value[0]), reverse=True)

            __key = 'asks'
            for i in range(0,len(current_data['data'][0][__key])):
                __temp_asks_d[current_data['data'][0][__key][i][0]] = (
                    current_data['data'][0][__key][i][1]
                )

            for i in range(0,len(diff_data['data'][0][__key])):
                if float(diff_data['data'][0][__key][i][1]) == 0:
                    n_t = __temp_asks_d.pop(diff_data['data'][0][__key][i][0],None)
                else:
                    __temp_asks_d[diff_data['data'][0][__key][i][0]] = (
                        diff_data['data'][0][__key][i][1]
                    )

            for i,j in __temp_asks_d.items():
                __temp_asks_l.append([i,j])

            __temp_asks = sorted(__temp_asks_l, key=lambda value: float(value[0]), reverse=False)

            current_data['data'][0]['bids'] = __temp_bids
            current_data['data'][0]['asks'] = __temp_asks
            current_data['data'][0]['ts'] = diff_data['data'][0]['ts']
            current_data['data'][0]['seqId'] = diff_data['data'][0]['seqId']
            self.__ws_temp_data[__stream_index] = current_data

            result = True

        return result
