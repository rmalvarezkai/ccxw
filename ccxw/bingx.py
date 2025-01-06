# pylint: disable=too-many-lines
"""
Ccxw - CryptoCurrency eXchange Websocket Library
Bingx auxiliary functions

Author: Ricardo Marcelo Alvarez
Date: 2023-10-31
"""

import os
import json
import time
import datetime
import gzip
import io
import math
import random
import threading
import websocket
import websocket_server

import ccxw.ccxw_common_functions as ccf
from ccxw.safe_thread_vars import DictSafeThread
import ccxw

class BingxCcxwAuxClass():
    """
    Ccxw - CryptoCurrency eXchange Websocket Library BingxCcxwAuxClass
    ==================================================================
        This class contains helper functions for the Ccxw class.
    """

    def __init__(self,\
                 streams: list[dict],\
                 trading_type: str='SPOT',\
                 testmode: bool=False,\
                 result_max_len: int=5,
                 data_max_len: int=1000,\
                 debug: bool=False):
        """
        BingxCcxwAuxClass constructor
        =============================
            :param self: BingxCcxwAuxClass instance.
            :param streams: list[dict]
                                    dicts must have this struct.
                                        {
                                            'endpoint': str only allowed 'order_book' | 'kline' |\
                                                'trades' | 'ticker',
                                            'symbol': str unified symbol.,
                                            'interval': str '1m' for 'kline' endpoint is\
                                                    mandatory.
                                        }

            :param trading_type: str only allowed 'SPOT'.
            :param testmode: bool.
            :param result_max_len: int Max return values > 1 and <= data_max_len.
            :param data_max_len: int. > 1 and <= 2500 max len of data getting from exchange.
            :param debug: bool Verbose output.

            :return: Return a new instance of the Class BingxCcxwAuxClass.
        """

        __exchange_limit_streams = 1024

        self.__exchange = os.path.basename(__file__)[:-3]
        self.__ws_streams = streams
        self.__ws_server = None
        self.__ws_server_url = '127.0.0.1'
        self.__ws_server_port_min = 10000
        self.__ws_server_port_max = 50000

        self.__ws_ping_interval = 0
        self.__ws_ping_timeout = None

        self.__ws_server = None
        self.__ws_server_url = '127.0.0.1'
        self.__ws_server_port_min = 10000
        self.__ws_server_port_max = 50000

        self.__ws_server_port = self.__ws_server_port_min

        self.__local_ws_url = 'ws://' + self.__ws_server_url + ':' + str(self.__ws_server_port)

        self.__testmode = testmode

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

        self.__ws_client = None
        self.__ws_endpoint_url_client = None
        self.__ws_endpoint_on_open_vars_client = None
        self.__ws_endpoint_on_close_vars_client = None

        self.__api_data_vars = None

        self.__ws_temp_data = DictSafeThread()
        self.__lock = threading.Lock()

        self.__thread_websocket_client = None
        self.__thread_api_client = None

        websocket.enableTrace(self.__debug)

        self.__is_stopped = False

        if not self.__check_streams_struct(streams):
            raise ValueError('The streams struct is not valid: ' + str(streams))

        if len(streams) > __exchange_limit_streams:
            raise ValueError('The exchange ' + str(self.__exchange)\
                             + ' not alowed more than ' + str(__exchange_limit_streams)\
                             + ' of streams.')

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
                                        'interval': str '1m' for 'kline' endpoint is\
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

    def reset_ws_temp_data(self):
        """
        reset_ws_temp_data
        ==================
        """
        for key_d in self.__ws_temp_data:
            self.__ws_temp_data[key_d] = None

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

    def __del__(self):
        with self.__lock:
            self.__is_stopped = True

        if self.__thread_websocket_client is not None\
            and threading.current_thread() is not self.__thread_websocket_client\
            and self.__thread_websocket_client.is_alive():
            try:
                self.__thread_websocket_client.join(9)
            except Exception: # pylint: disable=broad-except
                pass

        if self.__thread_api_client is not None\
            and threading.current_thread() is not self.__thread_api_client\
            and self.__thread_api_client.is_alive():
            try:
                self.__thread_api_client.join(9)
            except Exception: # pylint: disable=broad-except
                pass

    def start(self):
        """
        start
        =====

            :param self: OkxAuxClass instance.
        """
        with self.__lock:
            self.__is_stopped = False
        self.__start_ws_server()
        time.sleep(1)
        self.__start_ws_client()
        time.sleep(1)
        self.__start_get_data_from_api()

    def stop(self):
        """
        stop
        ====

            :param self: OkxAuxClass instance.
        """
        with self.__lock:
            self.__is_stopped = True
        self.__stop_ws_client()
        time.sleep(1)
        self.__stop_get_data_from_api()

        time.sleep(2)

        if self.__ws_server is not None:
            self.__ws_server.deny_new_connections()
            self.__ws_server.disconnect_clients_gracefully()
            time.sleep(1)
            self.__ws_server.shutdown_gracefully()
            self.__ws_server.disconnect_clients_gracefully()
            time.sleep(1)
            self.__ws_server.disconnect_clients_abruptly()
            #self.__ws_server.shutdown_abruptly()
            #self.__ws_server = None

    def __manage_websocket_open(self, ws):
        result = False

        if hasattr(ws, 'on_open_vars')\
            and ws.on_open_vars is not None\
            and isinstance(ws.on_open_vars, dict)\
            and all(key in ws.on_open_vars for key in ['id', 'reqType', 'dataType']):
            try:
                if ws.on_open_vars['id'] is not None and isinstance(ws.on_open_vars['id'], str)\
                    and ws.on_open_vars['reqType'] is not None\
                    and isinstance(ws.on_open_vars['reqType'], str)\
                    and ws.on_open_vars['dataType'] is not None:

                    if isinstance(ws.on_open_vars['dataType'], str):
                        __local_open_vars = None
                        __local_open_vars = json.dumps(ws.on_open_vars)
                        ws.send(__local_open_vars)
                        result = True
                    elif isinstance(ws.on_open_vars['dataType'], list):
                        for __local_data_type in ws.on_open_vars['dataType']:
                            __local_open_vars = None
                            __local_open_vars = {}
                            __local_open_vars['id'] = ws.on_open_vars['id']
                            __local_open_vars['reqType'] = ws.on_open_vars['reqType']
                            __local_open_vars['dataType'] = __local_data_type
                            __local_open_vars = json.dumps(__local_open_vars)

                            ws.send(__local_open_vars)
                            time.sleep(0.14)

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
                    print('Cerrando socket bingx')
            except Exception: # pylint: disable=broad-except
                pass

        if hasattr(ws, 'on_close_vars')\
            and ws.on_close_vars is not None\
            and isinstance(ws.on_close_vars, dict)\
            and all(key in ws.on_close_vars for key in ['id', 'reqType', 'dataType']):
            try:

                if ws.on_close_vars['id'] is not None and isinstance(ws.on_close_vars['id'], str)\
                    and ws.on_close_vars['reqType'] is not None\
                    and isinstance(ws.on_close_vars['reqType'], str)\
                    and ws.on_close_vars['dataType'] is not None:

                    if isinstance(ws.on_close_vars['dataType'], str):
                        __local_close_vars = None
                        __local_close_vars = json.dumps(ws.on_close_vars)
                        ws.send(__local_close_vars)
                        result = True
                    elif isinstance(ws.on_close_vars['dataType'], list):
                        for __local_data_type in ws.on_close_vars['dataType']:
                            __local_close_vars = None
                            __local_close_vars = {}
                            __local_close_vars['id'] = ws.on_close_vars['id']
                            __local_close_vars['reqType'] = ws.on_close_vars['reqType']
                            __local_close_vars['dataType'] = __local_data_type
                            __local_close_vars = json.dumps(__local_close_vars)
                            ws.send(__local_close_vars)
                            time.sleep(0.14)

                        result = True
            except Exception: # pylint: disable=broad-except
                result = False

        timer = threading.Timer(40, force_close)
        timer.start()
        timer.join(45)

        return result

    def __manage_websocket_message_local(self, ws, message): # pylint: disable=unused-argument

        if self.__ws_server is not None:
            if message is not None:
                if isinstance(message,bytes):
                    message = gzip.GzipFile(fileobj=io.BytesIO(message), mode='rb')
                    message = message.read()
                    message = message.decode('utf-8')

                    with self.__lock:
                        self.__ws_server.send_message_to_all(message)

    def __start_ws_client(self):
        result = True
        with self.__lock:
            self.__is_stopped = False
        self.__thread_api_client = threading.Thread(target=self.__start_ws)
        self.__thread_api_client.start()
        return result

    def __start_ws(self):
        result = False
        __socket = self.__ws_url + self.__ws_endpoint_url_client
        try:
            self.__ws_client = (
                websocket.WebSocketApp(__socket,\
                                       on_message=self.__manage_websocket_message_local,\
                                       on_open=self.__manage_websocket_open,\
                                       on_reconnect=self.__manage_websocket_reconnect,\
                                       on_close=self.__manage_websocket_close)
            )

            self.__ws_client.on_open_vars = self.__ws_endpoint_on_open_vars_client
            self.__ws_client.on_close_vars = self.__ws_endpoint_on_close_vars_client

            result = True

            __ws_temp = (
                self.__ws_client.run_forever(ping_interval=self.__ws_ping_interval,\
                                             ping_timeout=self.__ws_ping_timeout,\
                                             reconnect=30)
                )

        except Exception as exc: # pylint: disable=broad-except
            result = False
            print('On create websocket exception: ' + str(exc))

        return result

    def __start_get_data_from_api(self):
        result = True
        with self.__lock:
            self.__is_stopped = False
        self.__thread_api_client = threading.Thread(target=self.__get_data_from_api)
        self.__thread_api_client.start()
        return result

    def __stop_get_data_from_api(self):
        with self.__lock:
            self.__is_stopped = True

        if self.__thread_api_client is not None\
            and threading.current_thread() is not self.__thread_api_client\
            and self.__thread_api_client.is_alive():

            try:
                self.__thread_api_client.join(9)
            except Exception: # pylint: disable=broad-except
                pass

    def __stop_ws_client(self):
        with self.__lock:
            self.__is_stopped = True

        self.__ws_client.close()

        if self.__thread_websocket_client is not None\
            and threading.current_thread() is not self.__thread_websocket_client\
            and self.__thread_websocket_client.is_alive():

            try:
                self.__thread_websocket_client.join(9)
            except Exception: # pylint: disable=broad-except
                pass

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
                self.__ws_url = 'wss://open-api-ws.bingx.com'
                self.__ws_url_api = self.__local_ws_url
                self.__ws_url_test = self.__ws_url

            result = self.__ws_url_api
            if self.__testmode:
                result = self.__ws_url_test

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
            self.__url_api = 'https://open-api.bingx.com'
            self.__url_test = ''

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

            __l_endpoint = '/openApi/' + self.__trading_type.lower() + '/v1/common/symbols'

            __l_url_point = __l_url_api + __l_endpoint

            __data = ccf.file_get_contents_url(__l_url_point)

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
            and 'data' in __main_data and isinstance(__main_data['data'],dict)\
            and 'symbols' in __main_data['data']:
            if isinstance(__main_data['data']['symbols'],list):

                result = []
                for symbol_data in __main_data['data']['symbols']:
                    if symbol_data is not None and isinstance(symbol_data,dict)\
                        and 'symbol' in symbol_data and isinstance(symbol_data['symbol'],str):
                        result.append(\
                                        symbol_data['symbol'].replace('-','/').upper())

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
            if symbol.replace('-','').replace('/', '').lower()\
                == symbol_rpl.replace('-','').replace('/', '').lower():
                result = symbol_rpl
                break

        return result

    def __get_interval_from_unified_interval(self, interval):
        result = '1'

        if interval is not None:
            result = str(interval)

            if 'm' in result:
                result = result.replace('m', 'min')
            elif result == '1h':
                result = '60m'
            elif 'h' in result:
                result = result.replace('h', 'hour')
            elif 'd' in result:
                result = result.replace('d', 'day')
            elif 'w' in result:
                result = result.replace('w', 'week')
            elif 'mo' in result:
                result = result.replace('mo', 'mon')

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
            if result == '60m':
                result = '1h'
            elif 'min' in result:
                result = result.replace('min', 'm')
            elif 'hour' in result:
                result = result.replace('hour', 'h')
            elif 'day' in result:
                result = result.replace('day', 'd')
            elif 'week' in result:
                result = result.replace('week', 'w')
            elif 'mon' in result:
                result = result.replace('mon', 'mo')

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

        result = result.replace('/','').replace('-', '').lower()

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

        self.__ws_endpoint_url_client = '/market'
        self.__ws_endpoint_on_open_vars_client = None
        self.__ws_endpoint_on_close_vars_client = None
        self.__ws_endpoint_on_open_vars_client = {}
        self.__ws_endpoint_on_close_vars_client = {}
        self.__ws_endpoint_on_open_vars_client['id'] = str(time.time_ns())
        self.__ws_endpoint_on_open_vars_client['reqType'] = 'sub'
        self.__ws_endpoint_on_open_vars_client['dataType'] = None

        self.__ws_endpoint_on_close_vars_client['id'] = self.__ws_endpoint_on_open_vars_client['id']
        self.__ws_endpoint_on_close_vars_client['reqType'] = 'unsub'
        self.__ws_endpoint_on_close_vars_client['dataType'] = None

        self.__api_data_vars = None
        self.__api_data_vars = []

        __ws_args_client = None
        __ws_args_client = []

        __api_args_client = None
        __api_args_client = []

        for stream in self.__ws_streams:
            interval = 'none'

            if stream['endpoint'] == 'order_book':
                __ws_args_client.append(stream['symbol'].replace("/","-").upper()\
                                             + '@depth' + '100')

            elif stream['endpoint'] == 'kline':
                interval = stream['interval']
                __ws_args_client.append(stream['symbol'].replace("/","-").upper()\
                                             + '@kline_'\
                                             + self.__get_interval_from_unified_interval(interval))

            elif stream['endpoint'] == 'trades':
                __data_add = None
                __data_add = {}
                __data_add['endpoint'] = 'trades'
                __data_add['symbol'] = stream['symbol'].replace("/","-").upper()

                self.__api_data_vars.append(__data_add)

            elif stream['endpoint'] == 'ticker':
                __data_add = None
                __data_add = {}
                __data_add['endpoint'] = 'ticker'
                __data_add['symbol'] = stream['symbol'].replace("/","-").upper()

                self.__api_data_vars.append(__data_add)

            __stream_index = self.get_stream_index(stream['endpoint'],\
                                                     stream['symbol'],\
                                                     interval=interval)

            self.__ws_temp_data[__stream_index] = None

        self.__ws_endpoint_on_open_vars_client['dataType'] = __ws_args_client
        self.__ws_endpoint_on_close_vars_client['dataType'] = __ws_args_client

        result = {}
        result['ws_endpoint_url'] = self.__ws_endpoint_url
        result['ws_endpoint_on_open_vars'] = self.__ws_endpoint_on_open_vars
        result['ws_endpoint_on_close_vars'] = self.__ws_endpoint_on_close_vars

        return result

    def __get_data_from_api(self):
        """
        __get_data_from_api
        =================================
            This function get data from Rest API and send data to a local websocket server
            Requests limit 500 per minute and 1500 per 5 minutes -> 5 per second
            In our case 2 requests per second max.

                :return bool: True if all OK
        """

        result = True
        __min_time = 0.5
        __timeout = 9

        __to_stop = False

        with self.__lock:
            __to_stop = self.__is_stopped

        while not __to_stop:
            if self.__api_data_vars is not None\
                and isinstance(self.__api_data_vars, list)\
                and len(self.__api_data_vars) > 0:
                for __api_data_var in self.__api_data_vars:
                    __time_sleep = 0
                    __time_ini = time.time_ns()
                    if __api_data_var is not None and isinstance(__api_data_var, dict)\
                        and 'endpoint' in __api_data_var and 'symbol' in __api_data_var:
                        __data_url = None
                        __data_type = __api_data_var['symbol'] + '@' + __api_data_var['endpoint']

                        if __api_data_var['endpoint'] == 'trades':

                            __l_endpoint = '/openApi/' + str(self.__trading_type).lower()\
                                + '/v1/market/' + str(__api_data_var['endpoint'])\
                                + '?symbol=' + str(__api_data_var['symbol']) + '&limit=100'
                            __data_url = self.__url_api + __l_endpoint

                        elif __api_data_var['endpoint'] == 'ticker':
                            __timestamp = str(round(time.time_ns() / 1000000))
                            __l_endpoint = '/openApi/' + str(self.__trading_type).lower()\
                                + '/v1/' + str(__api_data_var['endpoint']) + '/24hr?timestamp='\
                                + __timestamp + '&symbol=' + str(__api_data_var['symbol'])
                            __data_url = self.__url_api + __l_endpoint

                        if __data_url is not None:
                            __message = (
                                ccf.file_get_contents_url(__data_url, 'b', None, {}, __timeout)
                            )

                            if __message is not None and ccf.is_json(__message):
                                __message = json.loads(__message)
                                if __message is not None\
                                    and isinstance(__message, dict)\
                                    and 'code' in __message\
                                    and 'data' in __message:
                                    __message_out = None
                                    __message_out = {}
                                    __message_out['code'] = __message['code']
                                    __message_out['data'] = __message['data']
                                    __message_out['timestamp'] = __message['timestamp']
                                    __message_out['dataType'] = __data_type
                                    __message_out['success'] = True
                                    __message_out = json.dumps(__message_out)
                                    self.__ws_server.send_message_to_all(__message_out)

                    __time_end = time.time_ns()
                    __time_diff = (__time_end - __time_ini) / 1000000000
                    __time_sleep = round((__min_time - __time_diff),3)

                    if __time_sleep > 0:
                        time.sleep(__time_sleep)
            else:
                time.sleep(1)

            with self.__lock:
                __to_stop = self.__is_stopped

        return result

    def manage_websocket_message_order_book(self, data, symbol):
        """
        manage_websocket_message_order_book
        ===================================
            This function manage websocket message and normalize result
            data for order_book endpoint.

                :param data: dict.
                :return dict: Return dict with normalized data.
        """
        result = None

        symbol = self.get_unified_symbol_from_symbol(symbol)
        __stream_index = self.get_stream_index('order_book', symbol)

        __temp_data = data
        __proc_data = False

        if __temp_data is not None and isinstance(__temp_data,dict)\
            and 'dataType' in __temp_data and 'data' in __temp_data\
            and isinstance(__temp_data['data'],dict):
            __bids = __temp_data['data']['bids']
            __asks = __temp_data['data']['asks']
            __asks.reverse()

            __message_out = None
            __message_out = {}
            __message_out['endpoint'] = 'order_book'
            __message_out['exchange'] = self.__exchange
            __message_out['symbol'] = symbol
            __message_out['interval'] = None
            __message_out['last_update_id'] = time.time_ns()
            __message_out['diff_update_id'] = 0
            __message_out['bids'] = __bids[:self.__result_max_len]
            __message_out['asks'] = __asks[:self.__result_max_len]
            __message_out['type'] = 'snapshot'
            __current_datetime = datetime.datetime.now(datetime.timezone.utc)
            __current_timestamp = __current_datetime.strftime("%s.%f")
            __current_datetime = __current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
            __message_out['timestamp'] = __current_timestamp
            __message_out['datetime'] = __current_datetime

            result = __message_out
            self.__ws_temp_data[__stream_index] = __temp_data

        return result

    def manage_websocket_message_kline(self, data, symbol, interval):
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

        symbol = self.get_unified_symbol_from_symbol(symbol)
        interval = self.get_unified_interval_from_interval(interval)

        __stream_index = self.get_stream_index('kline', symbol, interval)

        if self.__ws_temp_data[__stream_index] is None\
            or not isinstance(self.__ws_temp_data[__stream_index], dict):
            self.__ws_temp_data[__stream_index] = {}

        if __temp_data is not None and isinstance(__temp_data,dict)\
            and 'code' in __temp_data and int(__temp_data['code']) == 0\
            and 'dataType' in __temp_data:
            if 'data' in __temp_data and isinstance(__temp_data['data'],dict)\
                and 'E' in __temp_data['data'] and 'K' in __temp_data['data']\
                and isinstance(__temp_data['data']['K'],dict):

                __message_add = None
                __message_add = {}
                __message_add['endpoint'] = 'kline'
                __message_add['exchange'] = self.__exchange
                __message_add['symbol'] = symbol
                __message_add['interval'] = interval
                __message_add['last_update_id'] = __temp_data['data']['E']
                __message_add['open_time'] = int(__temp_data['data']['K']['t'])
                __message_add['close_time'] = int(__temp_data['data']['K']['T'])
                __message_add['open_time_date'] = (
                    time.strftime("%Y-%m-%d %H:%M:%S",\
                    time.gmtime(int(round(__message_add['open_time']/1000))))
                )
                __message_add['close_time_date'] = (
                    time.strftime("%Y-%m-%d %H:%M:%S",\
                    time.gmtime(int(round(__message_add['close_time']/1000))))
                )
                __message_add['open'] = __temp_data['data']['K']['o']
                __message_add['close'] = __temp_data['data']['K']['c']
                __message_add['hight'] = __temp_data['data']['K']['h']
                __message_add['low'] = __temp_data['data']['K']['l']
                __message_add['volume'] = __temp_data['data']['K']['v']
                __message_add['is_closed'] = None

                self.__ws_temp_data[__stream_index][int(__message_add['open_time'])] = __message_add

                while len(self.__ws_temp_data[__stream_index]) > self.__data_max_len:
                    __first_key = min(list(self.__ws_temp_data[__stream_index].keys()))
                    __nc = self.__ws_temp_data[__stream_index].pop(__first_key,None)

                __message_out = list(self.__ws_temp_data[__stream_index].values())

                result = __message_out[:self.__result_max_len]

        return result

    def manage_websocket_message_trades(self, data, symbol):
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

        symbol = self.get_unified_symbol_from_symbol(symbol)
        __stream_index = self.get_stream_index('trades', symbol)

        if __temp_data is not None and isinstance(__temp_data, dict)\
            and 'code' in __temp_data and  int(__temp_data['code']) == 0:
            if 'timestamp' in __temp_data and 'data' in __temp_data\
                and isinstance(__temp_data['data'],list)\
                and len(__temp_data['data']) > 0:

                if self.__ws_temp_data[__stream_index] is None:
                    self.__ws_temp_data[__stream_index] = {}

                for i in range(len(__temp_data['data']) - 1,-1,-1):
                    __message_add = None
                    __message_add = {}
                    __message_add['endpoint'] = 'trades'
                    __message_add['exchange'] = self.__exchange
                    __message_add['symbol'] = symbol
                    __message_add['interval'] = None
                    __message_add['event_time'] = __temp_data['timestamp']
                    __message_add['trade_id'] = str(__temp_data['data'][i]['id'])
                    __message_add['price'] = str(__temp_data['data'][i]['price'])
                    __message_add['quantity'] = str(__temp_data['data'][i]['qty'])
                    __message_add['trade_time'] = int(__temp_data['data'][i]['time'])
                    __message_add['trade_time_date'] = (
                        time.strftime("%Y-%m-%d %H:%M:%S",\
                        time.gmtime(int(round(__message_add['trade_time']/1000))))\
                            + '.' + str(round(math.modf(\
                                round(__message_add['trade_time']/1000,3))[0]*1000)\
                                    ).rjust(3,'0')
                    )

                    __side_of_taker = 'BUY'
                    if __temp_data['data'][i]['buyerMaker']:
                        __side_of_taker = 'SELL'
                    __message_add['side_of_taker'] = __side_of_taker

                    self.__ws_temp_data[__stream_index][int(__message_add['trade_id'])] = (
                        __message_add
                    )

                    while len(self.__ws_temp_data[__stream_index]) > self.__data_max_len:
                        __first_key = min(list(self.__ws_temp_data[__stream_index].keys()))
                        __nc = self.__ws_temp_data[__stream_index].pop(__first_key,None)

                    __message_out = list(self.__ws_temp_data[__stream_index].values())
                    #__message_out.reverse()
                    __message_out = __message_out[:self.__result_max_len]

                    result = __message_out

        return result

    def manage_websocket_message_ticker(self, data, symbol):
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

        symbol = self.get_unified_symbol_from_symbol(symbol)
        __stream_index = self.get_stream_index('ticker', symbol)

        if __temp_data is not None and isinstance(__temp_data,dict)\
            and 'code' in __temp_data and  int(__temp_data['code']) == 0:
            if 'timestamp' in __temp_data and 'data' in __temp_data\
                and isinstance(__temp_data['data'],list)\
                and len(__temp_data['data']) > 0:
                self.__ws_temp_data[__stream_index] = __temp_data

                i = 0
                __message_add = None
                __message_add = {}
                __message_add['endpoint'] = 'ticker'
                __message_add['exchange'] = self.__exchange
                __message_add['symbol'] = symbol
                __message_add['interval'] = None
                __message_add['event_type'] = '24hrTicker'
                __message_add['event_time'] = __temp_data['timestamp']
                __message_add['event_time_date'] = (
                    time.strftime("%Y-%m-%d %H:%M:%S",\
                        time.gmtime(int(round(int(__message_add['event_time'])/1000))))\
                        + '.' + str(round(math.modf(round(__message_add['event_time']/\
                            1000,3))[0]*1000)).rjust(3,'0')
                )
                __message_add['price_change'] = None
                __message_add['price_change_percent'] = None
                __message_add['weighted_average_price'] = None
                __message_add['first_trade_before_the_24hr_rolling_window'] = None
                __message_add['last_price'] = str(__temp_data['data'][i]['lastPrice'])
                __message_add['last_quantity'] = None
                __message_add['best_bid_price'] = None
                __message_add['best_bid_quantity'] = None
                __message_add['best_ask_price'] = None
                __message_add['best_ask_quantity'] = None
                __message_add['open_price'] = str(__temp_data['data'][i]['openPrice'])
                __message_add['high_price'] = str(__temp_data['data'][i]['highPrice'])
                __message_add['low_price'] = str(__temp_data['data'][i]['lowPrice'])
                __message_add['total_traded_base_asset_volume'] = (
                    str(__temp_data['data'][i]['volume'])
                )
                __message_add['total_traded_quote_asset_volume'] = (
                    str(__temp_data['data'][i]['quoteVolume'])
                )
                __message_add['statistics_open_time'] = (
                    __temp_data['data'][i]['openTime']
                )
                __message_add['statistics_open_time_date'] = (
                    time.strftime("%Y-%m-%d %H:%M:%S",\
                        time.gmtime(int(round(__message_add['statistics_open_time']/\
                            1000)))) + '.' +\
                                str(round(math.modf(\
                                    round(__message_add['statistics_open_time']/\
                                        1000,3))[0]*1000)).rjust(3,'0')
                )
                __message_add['statistics_close_time'] = (
                    __temp_data['data'][i]['closeTime']
                )
                __message_add['statistics_close_time_date'] = (
                    time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(int(\
                        round(__message_add['statistics_close_time']/1000))))\
                        + '.' + str(round(math.modf(\
                            round(__message_add['statistics_close_time']/\
                            1000,3))[0]*1000)).rjust(3,'0')
                )
                __message_add['total_number_of_trades'] = None

                __message_out = __message_add

                result = __message_out

        return result


    def manage_websocket_message(self, ws, message_in):
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
            if isinstance(message_in,bytes):
                message_in = gzip.GzipFile(fileobj=io.BytesIO(message_in), mode='rb')
                message_in = message_in.read()
                message_in = message_in.decode('utf-8')

            if message_in == 'Ping':
                ws.send('Pong')
                result = message_in
            else:
                if ccf.is_json(message_in):
                    __temp_data = json.loads(message_in)
                    __endpoint = 'NONE'
                    __symbol = None
                    __interval = None
                    __message_out = None

                    if __temp_data is not None\
                        and isinstance(__temp_data, dict):
                        if 'ping' in __temp_data\
                            and 'time' in __temp_data\
                            and __temp_data['ping'] is not None\
                            and __temp_data['time'] is not None:

                            __req_ping = {}
                            __req_ping['pong'] = __temp_data['ping']
                            __req_ping['time'] = __temp_data['time']

                            __req_ping = json.dumps(__req_ping)
                            ws.send(__req_ping)

                        elif  'code' in __temp_data\
                            and 'data' in __temp_data\
                            and 'dataType' in __temp_data\
                            and 'success' in __temp_data\
                            and __temp_data['success']:

                            __tmp_endpoint = 'NONE'

                            if '@' in __temp_data['dataType']:
                                __input_tmp = __temp_data['dataType'].split('@')
                                if __input_tmp is not None\
                                    and isinstance(__input_tmp, list)\
                                    and len(__input_tmp) >=2:
                                    __symbol = __input_tmp[0]
                                    __tmp_endpoint = __input_tmp[1]

                            if __tmp_endpoint.startswith('depth'):
                                __endpoint = 'order_book'
                            elif __tmp_endpoint.startswith('kline') and '_' in __tmp_endpoint:
                                __tmp_interval = __tmp_endpoint.split('_')
                                if __tmp_interval is not None\
                                    and isinstance(__tmp_interval, list)\
                                    and len(__tmp_interval) >=2:
                                    __endpoint = 'kline'
                                    __interval = __tmp_interval[1]
                            elif __tmp_endpoint.startswith('trades'):
                                __endpoint = 'trades'
                            elif __tmp_endpoint.startswith('ticker'):
                                __endpoint = 'ticker'

                            if __endpoint == 'order_book':
                                __message_out = (
                                    self.manage_websocket_message_order_book(__temp_data, __symbol)
                                )

                                if __message_out is not None:

                                    result = {}
                                    result['data'] = __message_out
                                    result['min_proc_time_ms'] = 0
                                    result['max_proc_time_ms'] = 0

                            elif __endpoint == 'kline':
                                __message_out = (
                                    self.manage_websocket_message_kline(__temp_data,\
                                                                        __symbol,\
                                                                        __interval)
                                )

                                if __message_out is not None:
                                    result = {}
                                    result['data'] = __message_out
                                    result['min_proc_time_ms'] = 0
                                    result['max_proc_time_ms'] = 0

                            elif __endpoint == 'trades':
                                __message_out = (
                                    self.manage_websocket_message_trades(__temp_data, __symbol)
                                )

                                if __message_out is not None:
                                    result = {}
                                    result['data'] = __message_out
                                    result['min_proc_time_ms'] = 0
                                    result['max_proc_time_ms'] = 0

                            elif __endpoint == 'ticker':
                                __message_out = (
                                    self.manage_websocket_message_ticker(__temp_data, __symbol)
                                )

                                if __message_out is not None:
                                    result = {}
                                    result['data'] = __message_out
                                    result['min_proc_time_ms'] = 0
                                    result['max_proc_time_ms'] = 0

        except Exception as exc: # pylint: disable=broad-except
            print('EXCEPTION: ' + str(exc))

        return result
