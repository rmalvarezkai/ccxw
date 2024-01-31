"""
Ccxw - CryptoCurrency eXchange Websocket Library
Kucoin auxiliary functions

Author: Ricardo Marcelo Alvarez
Date: 2023-10-31
"""

import os
import json
import time
import datetime
import queue
import math
import copy

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
                 data_max_len: int=1000,\
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
            :param data_max_len: int. > 1 and <= 400 max len of data getting from exchange.
            :param debug: bool Verbose output.

            :return: Return a new instance of the Class KucoinCcxwAuxClass.
        """

        __exchange_limit_streams = 100

        self.__exchange = os.path.basename(__file__)[:-3]
        self.__ws_streams = streams
        self.__testmode = testmode

        self.__exchange_info_cache = {}
        self.__exchange_info_cache['data'] = None
        self.__exchange_info_cache['last_get_time'] = 0

        self.__debug = debug # pylint: disable=unused-private-member

        self.__trading_type = trading_type
        self.__data_max_len = data_max_len

        self.__data_max_len = min(self.__data_max_len, 400)
        self.__data_max_len = max(self.__data_max_len, 1)

        self.__result_max_len = result_max_len

        self.__result_max_len = min(self.__result_max_len, 500)
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
        pass

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

    def get_websocket_url(self):
        """
        get_websocket_url
        =================
            This function set and return websocket URL.

                :return str: Return websocket URL.
        """

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
            __data_token = json.loads(__data_token_raw)

            if isinstance(__data_token,dict) and 'data' in __data_token\
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

                __topic_open['topic'] = '/market/level2:'\
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

    def __init_order_book_data(self, temp_data):

        result = False
        __data = None

        if 'symbol' in temp_data\
            and temp_data['symbol'] is not None\
            and isinstance(temp_data['symbol'], str):
            __symbol = temp_data['symbol']

            __url_dest = self.__url_api + '/api/v1/market/orderbook/level2_100?symbol='\
                + str(__symbol).replace("/","-").upper()


            __data = ccf.file_get_contents_url(__url_dest)

            if __data is not None and ccf.is_json(__data):
                __data = json.loads(__data)

                if __data is not None and isinstance(__data, dict)\
                    and 'code' in __data and 'data' in __data\
                    and __data['code'] == '200000':
                    __stream_index = self.get_stream_index('order_book', __symbol)
                    self.__ws_temp_data[__stream_index] = None
                    self.__ws_temp_data[__stream_index] = {}
                    self.__ws_temp_data[__stream_index]['endpoint'] = 'order_book'
                    self.__ws_temp_data[__stream_index]['exchange'] = self.__exchange
                    self.__ws_temp_data[__stream_index]['symbol'] = __symbol
                    self.__ws_temp_data[__stream_index]['interval'] = None
                    self.__ws_temp_data[__stream_index]['last_update_id'] = (
                        int(__data['data']['sequence'])
                    )
                    self.__ws_temp_data[__stream_index]['diff_update_id'] = 0
                    self.__ws_temp_data[__stream_index]['bids'] = __data['data']['bids']
                    self.__ws_temp_data[__stream_index]['asks'] = __data['data']['asks']
                    self.__ws_temp_data[__stream_index]['type'] = 'snapshot'

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

        if __temp_data is not None and isinstance(__temp_data,dict)\
            and 'type' in __temp_data and 'data' in __temp_data\
            and isinstance(__temp_data['data'],dict):
            if __temp_data['type'] == 'message':
                __symbol = __temp_data['topic'].split(':')[1]
                __stream_index = self.get_stream_index('order_book', __symbol)

                if 'changes' in __temp_data['data']\
                    and isinstance(__temp_data['data']['changes'],dict):
                    if self.__ws_temp_data[__stream_index] is None:
                        if self.__init_order_book_data(__temp_data['data'])\
                            and self.__manage_websocket_diff_data(__temp_data['data']):
                            __proc_data = True
                    elif self.__manage_websocket_diff_data(__temp_data['data']):
                        __proc_data = True

                if __proc_data:
                    __message_out = None
                    __message_out = {}
                    __message_out['endpoint'] = 'order_book'
                    __message_out['exchange'] = self.__exchange
                    __message_out['symbol'] = __symbol
                    __message_out['interval'] = None
                    __message_out['last_update_id'] = (
                        self.__ws_temp_data[__stream_index]['last_update_id']
                    )
                    __message_out['diff_update_id'] = (
                        self.__ws_temp_data[__stream_index]['diff_update_id']
                    )
                    __message_out['bids'] = (
                        self.__ws_temp_data[__stream_index]['bids'][:self.__result_max_len]
                    )
                    __message_out['asks'] = (
                        self.__ws_temp_data[__stream_index]['asks'][:self.__result_max_len]
                    )
                    __message_out['type'] = self.__ws_temp_data[__stream_index]['type']
                    __current_datetime = datetime.datetime.utcnow()
                    __current_timestamp = __current_datetime.strftime("%s.%f")
                    __current_datetime = (
                        __current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
                    )
                    __message_out['timestamp'] = __current_timestamp
                    __message_out['datetime'] = __current_datetime

                    result = __message_out

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
                __proc_data = False

                __endpoint = 'NONE'

                if __temp_data is not None\
                    and isinstance(__temp_data, dict)\
                    and 'topic' in __temp_data\
                    and __temp_data['topic'] is not None\
                    and isinstance(__temp_data['topic'], str)\
                    and len(__temp_data['topic']) > 0:

                    __tmp_endpoint = __temp_data['topic'].split(':')[0]

                    if __tmp_endpoint == '/market/level2':
                        __endpoint = 'order_book'
                    elif __tmp_endpoint == '/market/candles':
                        __endpoint = 'kline'
                    elif __tmp_endpoint == '/market/match':
                        __endpoint = 'trades'
                    elif __tmp_endpoint == '/market/ticker':
                        __endpoint = 'ticker'

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

    def __manage_websocket_diff_data(self, diff_data):

        result = False

        __symbol = None
        __stream_index = None

        current_data = None
        current_data_seq = 0

        if 'symbol' in diff_data\
            and diff_data['symbol'] is not None\
            and isinstance(diff_data['symbol'], str):
            __symbol = diff_data['symbol']
            __stream_index = self.get_stream_index('order_book', __symbol)

            current_data = self.__ws_temp_data[__stream_index]
            current_data_seq = current_data['last_update_id']

        if isinstance(diff_data['changes'],dict) and 'bids' in diff_data['changes']\
            and 'asks' in diff_data['changes'] and isinstance(diff_data['changes']['bids'],list)\
            and isinstance(diff_data['changes']['asks'],list) and __symbol is not None\
            and __stream_index is not None and current_data is not None:
            __temp_bids_d = {}
            __temp_asks_d = {}
            __temp_bids_l = []
            __temp_asks_l = []

            __key = 'bids'
            for i in range(0,len(current_data[__key])):
                __temp_bids_d[current_data[__key][i][0]] = current_data[__key][i][1]

            for i in range(0,len(diff_data['changes'][__key])):
                if int(diff_data['changes'][__key][i][2]) > current_data_seq:
                    if float(diff_data['changes'][__key][i][1]) == 0:
                        n_t = __temp_bids_d.pop(diff_data['changes'][__key][i][0],None) # pylint: disable=unused-variable
                    else:
                        __temp_bids_d[diff_data['changes'][__key][i][0]] = \
                            diff_data['changes'][__key][i][1]

            for i,j in __temp_bids_d.items():
                __temp_bids_l.append([i,j])

            __temp_bids = sorted(__temp_bids_l, key=lambda value: float(value[0]), reverse=True)

            __key = 'asks'
            for i in range(0,len(current_data[__key])):
                __temp_asks_d[current_data[__key][i][0]] = current_data[__key][i][1]

            for i in range(0,len(diff_data['changes'][__key])):
                if int(diff_data['changes'][__key][i][2]) > current_data_seq:
                    if float(diff_data['changes'][__key][i][1]) == 0:
                        n_t = __temp_asks_d.pop(diff_data['changes'][__key][i][0],None)
                    else:
                        __temp_asks_d[diff_data['changes'][__key][i][0]] = \
                            diff_data['changes'][__key][i][1]

            for i,j in __temp_asks_d.items():
                __temp_asks_l.append([i,j])

            __temp_asks = sorted(__temp_asks_l, key=lambda value: float(value[0]), reverse=False)

            current_data['bids'] = __temp_bids
            current_data['asks'] = __temp_asks
            current_data['last_update_id'] = int(diff_data['sequenceEnd'])
            current_data['diff_update_id'] = current_data['last_update_id'] - current_data_seq
            current_data['type'] = 'update'
            self.__ws_temp_data[__stream_index] = current_data
            result = True

        return result
