# pylint: disable=too-many-lines
"""
Ccxw - CryptoCurrency eXchange Websocket Library
Kucoin auxiliary functions

Author: Ricardo Marcelo Alvarez
Date: 2023-10-31
"""

import os
import threading
import json
import time
import datetime
import queue
import math
import copy
import pprint # pylint: disable=unused-import

import ccxw.ccxw_common_functions as ccf
from ccxw.safe_thread_vars import DictSafeThread
import ccxw

class KucoinCcxwAuxClass():
    """
    Ccxw - CryptoCurrency eXchange Websocket Library KucoinCcxwAuxClass
    ===================================================================
        This class contains helper functions for the Ccxw class.
    """

    def __init__(self,\
                 streams: list[dict],\
                 trading_type: str='SPOT',\
                 testmode: bool=False,\
                 result_max_len: int=5,\
                 data_max_len: int=2500,\
                 debug: bool=False):
        """
        KucoinCcxwAuxClass constructor
        ==============================
            :param self: KucoinCcxwAuxClass instance.
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

            :return: Return a new instance of the Class KucoinCcxwAuxClass.
        """

        __exchange_limit_streams = 100

        self.__exchange = os.path.basename(__file__)[:-3]
        self.__ws_streams = streams
        self.__testmode = testmode
        self.__ws_conn_id = None
        self.__ws_conn_id_lock = threading.Lock()
        self.__ping_thread = None
        self.__ws = None
        self.__ws_lock = threading.Lock()

        self.__stop_flag = False
        self.__stop_flag_lock = threading.Lock()

        self.__exchange_info_cache = {}
        self.__exchange_info_cache['data'] = None
        self.__exchange_info_cache['last_get_time'] = 0

        self.__debug = debug # pylint: disable=unused-private-member

        self.__trading_type = trading_type
        self.__data_max_len = data_max_len

        self.__data_max_len = min(self.__data_max_len, 2500)
        self.__data_max_len = max(self.__data_max_len, 1)

        self.__result_max_len = result_max_len

        self.__result_max_len = min(self.__result_max_len, self.__data_max_len)
        self.__result_max_len = max(self.__result_max_len, 1)

        self.__ws_url_api = None
        self.__ws_url_test = None
        self.__ws_url = None # pylint: disable=unused-private-member
        self.__url_api = None
        self.__url_test = None
        self.__ws_endpoint_url = None
        self.__ws_endpoint_on_open_vars = None
        self.__ws_endpoint_on_close_vars = None
        self.__ws_temp_data = DictSafeThread()

        self.ping_interval_ms = 10.0
        self.ping_timeout_ms = 10.0

        if not self.__check_streams_struct(streams):
            raise ValueError('The streams struct is not valid' + str(streams))

        if len(streams) > __exchange_limit_streams:
            raise ValueError('The exchange ' + str(self.__exchange)\
                             + ' not alowed more than ' + str(__exchange_limit_streams)\
                             + ' of streams.')

    def __del__(self):

        if not self.__stop_flag:
            self.stop()

    def reset_ws_temp_data(self):
        """
        reset_ws_temp_data
        ==================
        """
        for key_d in self.__ws_temp_data:
            self.__ws_temp_data[key_d] = None

    def start(self):
        """
        start
        =====
        """

        with self.__stop_flag_lock:
            self.__stop_flag = False

        self.__ping_thread = threading.Thread(target=self.__thread_ping,\
                                              name='ccxw_kucoin_ping_thread')

        self.__ping_thread.start()

    def stop(self):
        """
        stop
        ====
        """

        with self.__stop_flag_lock:
            self.__stop_flag = True

        if self.__ping_thread:
            self.__ping_thread.join(45)

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
                                            '3d' | '1w' | '1mo' only for 'kline' endpoint is\
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
                             '6h', '8h', '12h', '1d', '1w']

        return result

    def __get_websocket_token_raw(self):
        result = None

        self.get_api_url()
        __data_token_raw = None

        if self.__testmode:
            if self.__url_test is not None:
                __url_token = self.__url_test + '/api/v1/bullet-public'
                __data_token_raw = ccf.file_get_contents_url(__url_token,'b',{})
        else:
            if self.__url_api is not None:
                __url_token = self.__url_api + '/api/v1/bullet-public'
                __data_token_raw = ccf.file_get_contents_url(__url_token,'b',{})

        if __data_token_raw is not None and ccf.is_json(__data_token_raw):
            result = json.loads(__data_token_raw)

        return result

    def get_websocket_url(self):
        """
        get_websocket_url
        =================
            This function set and return websocket URL.

                :return str: Return websocket URL.
        """

        result = None

        __data_token = self.__get_websocket_token_raw()

        if __data_token is not None:

            if isinstance(__data_token, dict) and 'data' in __data_token\
                and isinstance(__data_token['data'],dict):
                __comp_0 = 'token' in __data_token['data']\
                    and isinstance(__data_token['data']['token'],str)\
                    and len(__data_token['data']['token']) > 0
                __comp_1 = 'instanceServers' in __data_token['data']\
                    and isinstance(__data_token['data']['instanceServers'],list)\
                    and len(__data_token['data']['instanceServers']) > 0\
                    and 'endpoint' in __data_token['data']['instanceServers'][0]\
                    and isinstance(__data_token['data']['instanceServers'][0]['endpoint'],str)\
                    and len(__data_token['data']['instanceServers'][0]['endpoint']) > 0

                if __comp_0 and __comp_1:
                    self.__ws_url_api = __data_token['data']['instanceServers'][0]['endpoint']
                    __ws_token = __data_token['data']['token']
                    self.__ws_url_api = self.__ws_url_api + '?token=' + __ws_token
                    self.__ws_url_test = self.__ws_url_api

                    if 'pingInterval' in __data_token['data']['instanceServers'][0]:
                        self.ping_interval_ms = (
                            float(int(__data_token['data']['instanceServers'][0]['pingInterval'])\
                                  / 1000)
                        )

                    if 'pingTimeout' in __data_token['data']['instanceServers'][0]:
                        self.ping_timeout_ms = (
                            float(int(__data_token['data']['instanceServers'][0]['pingTimeout'])\
                                  / 1000)
                        )

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
            self.__url_api = 'https://api.kucoin.com'
            self.__url_test = 'https://openapi-sandbox.kucoin.com'

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

            __l_endpoint = '/api/v2/symbols'

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
            and 'data' in __main_data and isinstance(__main_data['data'],list):

            result = []
            for symbol_data in __main_data['data']:
                if symbol_data is not None and isinstance(symbol_data,dict):
                    if 'baseCurrency' in symbol_data and 'quoteCurrency' in symbol_data\
                        and isinstance(symbol_data['baseCurrency'],str)\
                        and isinstance(symbol_data['quoteCurrency'],str):
                        result.append(\
                            symbol_data['baseCurrency'].upper() + '/'\
                            + symbol_data['quoteCurrency'].upper())

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
            if symbol.replace('-', '').lower() == symbol_rpl.replace('/', '').lower():
                result = symbol_rpl
                break

        return result

    def __get_interval_from_unified_interval(self, interval):
        result = '1'

        if interval is not None:
            result = str(interval)

            if 'm' in result:
                result = result.replace('m', 'min')
            elif 'h' in result or 'H' in result:
                result = result.lower().replace('h', 'hour')
                result = 60 * int(result)
            elif 'd' in result or 'D' in result:
                result = result.lower().replace('d', 'day')
            elif 'w' in result or 'W' in result:
                result = result.lower().replace('w', 'week')

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
            result = str(interval)

            if 'min' in result:
                result = result.replace('min', 'm')
            elif 'hour' in result:
                result = result.replace('hour', 'h')
            elif 'day' in result:
                result = result.replace('day', 'd')
            elif 'week' in result:
                result = result.replace('week', 'w')

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

        result = result + '_' + endpoint + '_'\
            + self.get_unified_symbol_from_symbol(symbol) + '_'\
            + self.get_unified_interval_from_interval(interval)

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

        self.__ws_endpoint_on_open_vars = []
        self.__ws_endpoint_on_close_vars = []

        self.__ws_endpoint_url = ''
        __id = str(time.time_ns())
        self.__ws_endpoint_url = '&connectId=' + __id

        for stream in self.__ws_streams:
            interval = 'none'

            __topic_open = None
            __topic_open = {}
            __topic_open['id'] = __id
            __topic_open['type'] = 'subscribe'
            __topic_open['topic'] = ''
            __topic_open['privateChannel'] = False
            __topic_open['response'] = True

            __topic_close = None
            __topic_close = copy.copy(__topic_open)
            __topic_close['type'] = 'unsubscribe'
            time.sleep(0.050)

            if stream['endpoint'] == 'order_book':
                result = True

                __topic_open['topic'] = '/spotMarket/level2Depth50:'\
                    + stream['symbol'].replace("/","-").upper()
                __topic_close['topic'] = __topic_open['topic']

            elif stream['endpoint'] == 'kline':
                interval = stream['interval']

                __topic_open['topic'] = '/market/candles:'\
                    + stream['symbol'].replace("/","-").upper()\
                    + '_' + self.__get_interval_from_unified_interval(interval)
                __topic_close['topic'] = __topic_open['topic']


            elif stream['endpoint'] == 'trades':
                __topic_open['topic'] = '/market/match:' + stream['symbol'].replace("/","-").upper()
                __topic_close['topic'] = __topic_open['topic']

            elif stream['endpoint'] == 'ticker':
                __topic_open['topic'] = '/market/ticker:'\
                    + stream['symbol'].replace("/","-").upper()
                __topic_close['topic'] = __topic_open['topic']

            __stream_index = self.get_stream_index(stream['endpoint'],\
                                                     stream['symbol'],\
                                                     interval=interval)
            self.__ws_temp_data[__stream_index] = None
            self.__ws_endpoint_on_open_vars.append(json.dumps(__topic_open))
            self.__ws_endpoint_on_close_vars.append(json.dumps(__topic_close))


        result = {}
        result['ws_endpoint_url'] = self.__ws_endpoint_url
        result['ws_endpoint_on_open_vars'] = self.__ws_endpoint_on_open_vars
        result['ws_endpoint_on_close_vars'] = self.__ws_endpoint_on_close_vars
        result['ws_ping_interval'] = self.ping_interval_ms
        result['ws_ping_timeout'] = self.ping_timeout_ms

        return result

    def __send_ping(self):
        result = False

        __local_con_id = None

        with self.__ws_conn_id_lock:
            __local_con_id = self.__ws_conn_id

        if __local_con_id is not None:
            __id = ccf.random_string(9) + str(round(time.time() * 1000000))
            __req_ping = {
                'id': __id,
                'type': 'ping'
            }

            __req_ping = json.dumps(__req_ping, sort_keys=False)

            try:
                with self.__ws_lock:
                    if self.__ws is not None:
                        self.__ws.send(__req_ping)
                result = True
            except Exception: # pylint: disable=broad-except
                result = False

        return result

    def __thread_ping(self):

        __local_stop = False
        __local_con_id = None
        __last_ping_time = 0
        __sleep_time = 0

        __token_time = 0
        __token_time_limit = 1800

        while not __local_stop:
            if abs(time.time() - __token_time) >= __token_time_limit:
                __token_data = self.__get_websocket_token_raw()
                if __token_data is not None and isinstance(__token_data, dict):
                    __token_time = time.time()

            with self.__ws_conn_id_lock:
                __local_con_id = self.__ws_conn_id_lock

            if __local_con_id is not None:
                if self.__send_ping():
                    __sleep_time = self.ping_interval_ms - abs(time.time() - __last_ping_time)
                    __sleep_time -= 1
                    __last_ping_time = time.time()
                else:
                    __sleep_time = 1

                if __sleep_time <= 0:
                    __sleep_time = 1
            else:
                __sleep_time = self.ping_interval_ms

            time.sleep(__sleep_time)


            with self.__stop_flag_lock:
                __local_stop = self.__stop_flag

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

        if __temp_data is not None and isinstance(__temp_data,dict)\
            and 'type' in __temp_data and 'data' in __temp_data\
            and isinstance(__temp_data['data'],dict):
            if __temp_data['type'] == 'message':
                __symbol = None
                __tmp_split = __temp_data['topic'].split(':')

                if __tmp_split is not None\
                    and isinstance(__tmp_split, list)\
                    and len(__tmp_split) > 1:
                    __symbol = __tmp_split[1]
                __stream_index = self.get_stream_index('order_book', __symbol)

                if __symbol is not None\
                    and 'asks' in __temp_data['data']\
                    and 'bids' in __temp_data['data']\
                    and 'timestamp' in __temp_data['data']:
                    __bids = __temp_data['data']['bids']
                    __asks = __temp_data['data']['asks']

                    __message_out = None
                    __message_out = {}
                    __message_out['endpoint'] = 'order_book'
                    __message_out['exchange'] = self.__exchange
                    __message_out['symbol'] = __symbol
                    __message_out['interval'] = None
                    __message_out['last_update_id'] = __temp_data['data']['timestamp']
                    __message_out['diff_update_id'] = 0

                    __message_out['bids'] = __bids[:self.__result_max_len]
                    __message_out['asks'] = __asks[:self.__result_max_len]
                    __message_out['type'] = 'snapshot'
                    __current_datetime = datetime.datetime.now(datetime.timezone.utc)
                    __current_timestamp = __current_datetime.strftime("%s.%f")
                    __current_datetime = (
                        __current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
                    )
                    __message_out['timestamp'] = __current_timestamp
                    __message_out['datetime'] = __current_datetime

                    result = __message_out
                    self.__ws_temp_data[__stream_index] = __temp_data

        return result

    def manage_websocket_message_kline(self, data):
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

        __symbol = None
        __interval = None

        if __temp_data is not None and isinstance(__temp_data, dict):
            __interval = __temp_data['topic'].split('_')[1]

            if 'type' in __temp_data and 'data' in __temp_data\
                and isinstance(__temp_data['data'],dict):
                if 'candles' in __temp_data['data']\
                    and isinstance(__temp_data['data']['candles'],list)\
                    and len(__temp_data['data']['candles']) > 0\
                    and 'symbol' in __temp_data['data']:
                    __symbol = __temp_data['data']['symbol']
                    __stream_index = self.get_stream_index('kline', __symbol, __interval)

                    __delta_time = 60000

                    if 'min' in __interval:
                        __delta_mult = int(__interval.replace('min',''))
                        __delta_time = 60000 * __delta_mult

                    elif 'hour' in __interval:
                        __delta_mult = int(__interval.replace('hour',''))
                        __delta_time = 60000 * __delta_mult * 60

                    elif 'day' in __interval:
                        __delta_mult = int(__interval.replace('day',''))
                        __delta_time = 60000 * __delta_mult * 60 * 24

                    elif 'week' in __interval:
                        __delta_mult = int(__interval.replace('week',''))
                        __delta_time = 60000 * __delta_mult * 60 * 24

                    __delta_time = __delta_time - 1

                    if self.__ws_temp_data[__stream_index] is None\
                        or not isinstance(self.__ws_temp_data[__stream_index], dict):
                        self.__ws_temp_data[__stream_index] = {}

                    __message_add = None
                    __message_add = {}
                    __message_add['endpoint'] = 'kline'
                    __message_add['exchange'] = self.__exchange
                    __message_add['symbol'] = __symbol
                    __message_add['interval'] = __interval
                    __message_add['last_update_id'] = __temp_data['data']['time']
                    __message_add['open_time'] = int(__temp_data['data']['candles'][0]) * 1000
                    __message_add['close_time'] = (
                        (int(__temp_data['data']['candles'][0]) * 1000) + __delta_time
                    )
                    __message_add['open_time_date'] = (
                        time.strftime("%Y-%m-%d %H:%M:%S",\
                                        time.gmtime(int(round(__message_add['open_time']/1000))))
                    )
                    __message_add['close_time_date'] = (
                        time.strftime("%Y-%m-%d %H:%M:%S",\
                                        time.gmtime(int(round(__message_add['close_time']/1000))))
                    )
                    __message_add['open'] = __temp_data['data']['candles'][1]
                    __message_add['close'] = __temp_data['data']['candles'][2]
                    __message_add['hight'] = __temp_data['data']['candles'][3]
                    __message_add['low'] = __temp_data['data']['candles'][4]
                    __message_add['volume'] = __temp_data['data']['candles'][5]
                    __message_add['is_closed'] = None

                    self.__ws_temp_data[__stream_index][int(__message_add['open_time'])] = (
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

        if __temp_data is not None and isinstance(__temp_data, dict):
            if 'type' in __temp_data and __temp_data['type'] == 'message'\
                and  'data' in __temp_data and isinstance(__temp_data['data'], dict)\
                and 'symbol' in __temp_data['data']:

                __symbol = __temp_data['data']['symbol']
                __stream_index = self.get_stream_index('trades', __symbol)

                if self.__ws_temp_data[__stream_index] is None:
                    self.__ws_temp_data[__stream_index] = queue.Queue(maxsize=self.__data_max_len)

                __message_add = None
                __message_add = {}
                __message_add['endpoint'] = 'trades'
                __message_add['exchange'] = self.__exchange
                __message_add['symbol'] = __symbol
                __message_add['interval'] = None
                __message_add['event_time'] = int(round(time.time_ns() / 1000000))
                __message_add['trade_id'] = str(__temp_data['data']['tradeId'])
                __message_add['price'] = str(__temp_data['data']['price'])
                __message_add['quantity'] = str(__temp_data['data']['size'])
                __message_add['trade_time'] = (
                    int(round(float(__temp_data['data']['time']) / 1000000))
                )
                __message_add['trade_time_date'] = (
                    time.strftime("%Y-%m-%d %H:%M:%S",\
                                    time.gmtime(int(round(__message_add['trade_time']\
                                                        /1000))))\
                        + '.' + str(round(math.modf(round(__message_add['trade_time']/1000,\
                                                            3))[0]*1000)).rjust(3,'0')
                )
                __message_add['side_of_taker'] = __temp_data['data']['side'].upper()

                if self.__ws_temp_data[__stream_index].full():
                    self.__ws_temp_data[__stream_index].get(True,1)

                self.__ws_temp_data[__stream_index].put(__message_add,True,5)

                __message_out = list(self.__ws_temp_data[__stream_index].queue)
                ##__message_out.reverse()

                result = __message_out[:self.__result_max_len]

        return result

    def manage_websocket_message_ticker(self, data):
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

        if __temp_data is not None and isinstance(__temp_data,dict):
            if 'type' in __temp_data and __temp_data['type'] == 'message'\
                and  'data' in __temp_data and isinstance(__temp_data['data'],dict):

                __symbol = __temp_data['topic'].split(':')[1]
                __stream_index = self.get_stream_index('ticker', __symbol)

                self.__ws_temp_data[__stream_index] = __temp_data

                __message_add = None
                __message_add = {}
                __message_add['endpoint'] = 'ticker'
                __message_add['exchange'] = self.__exchange
                __message_add['symbol'] = __symbol
                __message_add['interval'] = None
                __message_add['event_type'] = '24hrTicker'
                __message_add['event_time'] = int(round(time.time_ns() / 1000000))
                __message_add['event_time_date'] = (
                    time.strftime("%Y-%m-%d %H:%M:%S",\
                                    time.gmtime(int(round(__message_add['event_time']\
                                                        /1000))))\
                        + '.' + str(round(math.modf(round(__message_add['event_time']\
                                                            /1000,3))[0]*1000)).rjust(3,'0')
                )
                __message_add['price_change'] = None
                __message_add['price_change_percent'] = None
                __message_add['weighted_average_price'] = None
                __message_add['first_trade_before_the_24hr_rolling_window'] = None
                __message_add['last_price'] = __temp_data['data']['price']
                __message_add['last_quantity'] = __temp_data['data']['size']
                __message_add['best_bid_price'] = __temp_data['data']['bestBid']
                __message_add['best_bid_quantity'] = __temp_data['data']['bestBidSize']
                __message_add['best_ask_price'] = __temp_data['data']['bestAsk']
                __message_add['best_ask_quantity'] = __temp_data['data']['bestAskSize']
                __message_add['open_price'] = None
                __message_add['high_price'] = None
                __message_add['low_price'] = None
                __message_add['total_traded_base_asset_volume'] = None
                __message_add['total_traded_quote_asset_volume'] = None
                __message_add['statistics_open_time'] = None
                __message_add['statistics_open_time_date'] = None
                __message_add['statistics_close_time'] = None
                __message_add['statistics_close_time_date'] = None
                __message_add['total_number_of_trades'] = None

                __message_out = __message_add

                result = __message_out

        return result

    def manage_websocket_message(self, ws, message_in): # pylint: disable=unused-argument
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
            with self.__ws_lock:
                self.__ws = ws

            if ccf.is_json(message_in):
                __temp_data = json.loads(message_in)
                __proc_data = False

                __endpoint = 'NONE'

                if __temp_data is not None\
                    and isinstance(__temp_data, dict):
                    if 'topic' in __temp_data\
                        and __temp_data['topic'] is not None\
                        and isinstance(__temp_data['topic'], str)\
                        and len(__temp_data['topic']) > 0:

                        __tmp_endpoint = 'NONE'
                        __tmp_split = __temp_data['topic'].split(':')

                        if __tmp_split is not None and isinstance(__tmp_split, list):
                            if len(__tmp_split) > 0:
                                __tmp_endpoint = __temp_data['topic'].split(':')[0]

                        if __tmp_endpoint == '/spotMarket/level2Depth50':
                            __endpoint = 'order_book'
                        elif __tmp_endpoint == '/market/candles':
                            __endpoint = 'kline'
                        elif __tmp_endpoint == '/market/match':
                            __endpoint = 'trades'
                        elif __tmp_endpoint == '/market/ticker':
                            __endpoint = 'ticker'
                    elif 'type' in __temp_data\
                        and __temp_data['type'] is not None\
                        and isinstance(__temp_data['type'], str)\
                        and 'id' in __temp_data\
                        and __temp_data['id'] is not None\
                        and isinstance(__temp_data['id'], str):

                        if __temp_data['type'] == 'welcome':
                            with self.__ws_conn_id_lock:
                                self.__ws_conn_id = __temp_data['id']

                if __endpoint == 'order_book':
                    __message_out = self.manage_websocket_message_order_book(__temp_data)

                    result = {}
                    result['data'] = __message_out
                    result['min_proc_time_ms'] = 0
                    result['max_proc_time_ms'] = 0

                elif __endpoint == 'kline':
                    __message_out = self.manage_websocket_message_kline(__temp_data)

                    result = {}
                    result['data'] = __message_out
                    result['min_proc_time_ms'] = 0
                    result['max_proc_time_ms'] = 0

                elif __endpoint == 'trades':
                    __message_out = self.manage_websocket_message_trades(__temp_data)

                    result = {}
                    result['data'] = __message_out
                    result['min_proc_time_ms'] = 0
                    result['max_proc_time_ms'] = 0

                elif __endpoint == 'ticker':
                    __message_out = self.manage_websocket_message_ticker(__temp_data)

                    result = {}
                    result['data'] = __message_out
                    result['min_proc_time_ms'] = 0
                    result['max_proc_time_ms'] = 0

        except Exception as exc: # pylint: disable=broad-except
            print(str(exc))

        return result
