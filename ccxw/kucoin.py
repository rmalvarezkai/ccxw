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

import ccxw.ccxw_common_functions as ccf

class KucoinAuxClass(): # pylint: disable=too-many-instance-attributes, duplicate-code
    """
    Ccxw - CryptoCurrency eXchange Websocket Library KucoinAuxClass
    ===============================================================
        This class contains helper functions for the Ccxw class.
    """
    # pylint: disable=too-many-arguments
    def __init__(self, endpoint: str=None, symbol: str=None, trading_type: str='SPOT',\
                 testmode: bool=False, api_key: str=None, api_secret: str=None,\
                 result_max_len: int=5, update_speed: str='100ms', interval: str='1m',\
                 data_max_len: int=1000, debug: bool=False):
        """
        KucoinAuxClass constructor
        ==========================
            Initializes the KucoinAuxClass with the provided parameters.

                :param self: KucoinAuxClass instance.
                :param endpoint: str.
                :param symbol: str unified symbol.
                :param trading_type: str only allowed 'SPOT'.
                :param testmode: bool.
                :param api_key: str Not necesary only for future features.
                :param api_secret: str Not necesary only for future features.
                :param result_max_len: int Max return values > 1 and <= trades_max_len.
                :param update_speed: str only allowed '100ms' | '1000ms' Only for some endpoints.
                :param interval: str only allowed '1m' | '3m' | '5m' | '15m' | '30m' | '1H' | '2H' 
                    | '4H' | '6H' | '8H' | '12H' | '1D' | '3D' | '1W' | '1M'.
                :param data_max_len: int. > 1 and <= 400 max len of data getting from exchange.
                :param debug: bool Verbose output.

                :return: Return a new instance of the Class KucoinAuxClass.
        """

        self.__exchange = os.path.basename(__file__)[:-3]
        self.__ws_endpoint = endpoint
        self.__api_key = api_key # pylint: disable=unused-private-member
        self.__api_secret = api_secret # pylint: disable=unused-private-member
        self.__testmode = testmode

        self.__exchange_info_cache = {}
        self.__exchange_info_cache['data'] = None
        self.__exchange_info_cache['last_get_time'] = 0

        self.__debug = debug # pylint: disable=unused-private-member
        self.__ws_symbol = symbol
        self.__trading_type = trading_type
        self.__data_max_len = data_max_len

        self.__data_max_len = min(self.__data_max_len, 400)
        self.__data_max_len = max(self.__data_max_len, 1)

        self.__result_max_len = result_max_len
        self.__update_speed = update_speed
        self.__interval = interval

        self.__result_max_len = min(self.__result_max_len, 500)
        self.__result_max_len = max(self.__result_max_len, 1)

        self.__exchange_hostname = None # pylint: disable=unused-private-member
        self.__ws_url_api = None
        self.__ws_url_test = None
        self.__ws_url = None # pylint: disable=unused-private-member
        self.__url_api = None
        self.__url_test = None
        self.__ws_endpoint_url = None
        self.__ws_endpoint_on_open_vars = None
        self.__ws_endpoint_on_close_vars = None
        self.__listen_key = None # pylint: disable=unused-private-member
        self.__ws_temp_data = None

        self.ping_interval_ms = 10.0
        self.ping_timeout_ms = 10.0

    def __del__(self):
        pass

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

    def get_exchange_info(self, full_list=False): # pylint: disable=unused-argument
        """
        get_exchange_info
        =================
            This function get exchange info. 

                :return dict: Return exchange info.
        """

        result = None

        full_list = True
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
        __main_data = self.get_exchange_info(True)

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

    def if_symbol_supported(self):
        """
        if_symbol_supported
        ===================
            This function check if symbol is supported by the exchange.

                :return bool: Return True if supported 
        """

        result = False

        __data = self.get_exchange_full_list_symbols()

        if isinstance(__data,list) and self.__ws_symbol in __data:
            result = True

        return result

    def get_websocket_endpoint_path(self): # pylint: disable=too-many-statements
        """
        get_websocket_endpoint_path
        ===========================
            This function set endpoint URL and on_open vars and on_close vars.

                :return dict: Return dict with endpoint, open_vars and close_vars
        """

        result = None

        if not self.__update_speed in ('100ms', '1000ms'):
            self.__update_speed = '1000ms'

        __send_data_vars = None

        if self.__ws_endpoint == 'order_book':
            __send_data_vars = None
            __send_data_vars = {}
            __send_data_vars['id'] = str(time.time_ns())
            __send_data_vars['type'] = 'subscribe'
            __send_data_vars['topic'] = '/market/level2:'\
                + self.__ws_symbol.replace("/","-").upper()
            __send_data_vars['privateChannel'] = False
            __send_data_vars['response'] = True

            self.__ws_endpoint_url = ''
            self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)

        elif self.__ws_endpoint == 'kline':
            __send_data_vars = None
            __send_data_vars = {}
            __send_data_vars['id'] = str(time.time_ns())
            __send_data_vars['type'] = 'subscribe'
            __send_data_vars['topic'] = '/market/candles:'\
                + self.__ws_symbol.replace("/","-").upper()\
                + '_' + self.__interval.replace("m","min").replace("h","hour")\
                    .replace("d","day").replace("w","week")

            __send_data_vars['privateChannel'] = False
            __send_data_vars['response'] = True

            self.__ws_endpoint_url = ''
            self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)

        elif self.__ws_endpoint == 'trades':
            __send_data_vars = None
            __send_data_vars = {}
            __send_data_vars['id'] = str(time.time_ns())
            __send_data_vars['type'] = 'subscribe'
            __send_data_vars['topic'] = '/market/match:' + self.__ws_symbol.replace("/","-").upper()
            __send_data_vars['privateChannel'] = False
            __send_data_vars['response'] = True

            self.__ws_endpoint_url = ''
            self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)

        elif self.__ws_endpoint == 'ticker':
            __send_data_vars = None
            __send_data_vars = {}
            __send_data_vars['id'] = str(time.time_ns())
            __send_data_vars['type'] = 'subscribe'
            __send_data_vars['topic'] = '/market/ticker:'\
                + self.__ws_symbol.replace("/","-").upper()
            __send_data_vars['privateChannel'] = False
            __send_data_vars['response'] = True

            self.__ws_endpoint_url = ''
            self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)

        if __send_data_vars is not None and isinstance(__send_data_vars,dict):
            self.__ws_endpoint_on_close_vars = __send_data_vars
            self.__ws_endpoint_on_close_vars['type'] = 'unsubscribe'
            self.__ws_endpoint_on_close_vars = json.dumps(self.__ws_endpoint_on_close_vars)

        result = {}
        result['ws_endpoint_url'] = self.__ws_endpoint_url
        result['ws_endpoint_on_open_vars'] = self.__ws_endpoint_on_open_vars
        result['ws_endpoint_on_close_vars'] = self.__ws_endpoint_on_close_vars
        result['ws_ping_interval'] = self.ping_interval_ms
        result['ws_ping_timeout'] = self.ping_timeout_ms

        return result

    def __init_order_book_data(self):

        result = False

        __url_dest = self.__url_api + '/api/v1/market/orderbook/level2_100?symbol='\
            + str(self.__ws_symbol).replace("/","-").upper()

        __data = ccf.file_get_contents_url(__url_dest)

        if __data is not None and ccf.is_json(__data):
            __data = json.loads(__data)

            if isinstance(__data,dict) and 'code' in __data and 'data' in __data\
                and __data['code'] == '200000':
                self.__ws_temp_data = None
                self.__ws_temp_data = {}
                self.__ws_temp_data['endpoint'] = self.__ws_endpoint
                self.__ws_temp_data['exchange'] = self.__exchange
                self.__ws_temp_data['symbol'] = self.__ws_symbol
                self.__ws_temp_data['interval'] = None
                self.__ws_temp_data['last_update_id'] = int(__data['data']['sequence'])
                self.__ws_temp_data['diff_update_id'] = 0
                self.__ws_temp_data['bids'] = __data['data']['bids']
                self.__ws_temp_data['asks'] = __data['data']['asks']
                self.__ws_temp_data['type'] = 'snapshot'

                result = True

        return result

    def manage_websocket_message_order_book(self,data):
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

                if 'changes' in __temp_data['data']\
                    and isinstance(__temp_data['data']['changes'],dict):
                    if self.__ws_temp_data is None:
                        if self.__init_order_book_data()\
                            and self.__manage_websocket_diff_data(__temp_data['data']):
                            __proc_data = True
                    elif self.__manage_websocket_diff_data(__temp_data['data']):
                        __proc_data = True

                if __proc_data:
                    __message_out = None
                    __message_out = {}
                    __message_out['endpoint'] = self.__ws_endpoint
                    __message_out['exchange'] = self.__exchange
                    __message_out['symbol'] = self.__ws_symbol
                    __message_out['interval'] = None
                    __message_out['last_update_id'] = (
                        self.__ws_temp_data['last_update_id']
                    )
                    __message_out['diff_update_id'] = (
                        self.__ws_temp_data['diff_update_id']
                    )
                    __message_out['bids'] = (
                        self.__ws_temp_data['bids'][:self.__result_max_len]
                    )
                    __message_out['asks'] = (
                        self.__ws_temp_data['asks'][:self.__result_max_len]
                    )
                    __message_out['type'] = self.__ws_temp_data['type']
                    __current_datetime = datetime.datetime.utcnow()
                    __current_timestamp = __current_datetime.strftime("%s.%f")
                    __current_datetime = (
                        __current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
                    )
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

        if __temp_data is not None and isinstance(__temp_data,dict):
            if 'type' in __temp_data and 'data' in __temp_data\
                and isinstance(__temp_data['data'],dict):
                if 'candles' in __temp_data['data']\
                    and isinstance(__temp_data['data']['candles'],list)\
                    and len(__temp_data['data']['candles']) > 0:

                    __delta_time = 60000

                    if self.__interval[-1] == 'm':
                        __delta_mult = int(self.__interval.replace('m',''))
                        __delta_time = 60000 * __delta_mult

                    elif self.__interval[-1] == 'h' or self.__interval[-1] == 'H':
                        __delta_mult = int(self.__interval.replace('h','').replace('H',''))
                        __delta_time = 60000 * __delta_mult * 60

                    elif self.__interval[-1] == 'd' or self.__interval[-1] == 'D':
                        __delta_mult = int(self.__interval.replace('d','').replace('D',''))
                        __delta_time = 60000 * __delta_mult * 60 * 24

                    elif self.__interval[-1] == 'w' or self.__interval[-1] == 'W':
                        __delta_mult = int(self.__interval.replace('w','').replace('W',''))
                        __delta_time = 60000 * __delta_mult * 60 * 24

                    __delta_time = __delta_time - 1

                    if self.__ws_temp_data is None or not isinstance(self.__ws_temp_data,dict):
                        self.__ws_temp_data = {}

                    __message_add = None
                    __message_add = {}
                    __message_add['endpoint'] = self.__ws_endpoint
                    __message_add['exchange'] = self.__exchange
                    __message_add['symbol'] = self.__ws_symbol
                    __message_add['interval'] = self.__interval
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

                    self.__ws_temp_data[int(__message_add['open_time'])] = __message_add

                    while len(self.__ws_temp_data) > self.__data_max_len:
                        __first_key = min(list(self.__ws_temp_data.keys()))
                        __nc = self.__ws_temp_data.pop(__first_key,None)

                    __message_out = list(self.__ws_temp_data.values())

                    result = __message_out

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

        if __temp_data is not None and isinstance(__temp_data,dict):
            if 'type' in __temp_data and __temp_data['type'] == 'message'\
                and  'data' in __temp_data and isinstance(__temp_data['data'],dict):

                if self.__ws_temp_data is None:
                    self.__ws_temp_data = queue.Queue(maxsize=self.__data_max_len)

                __message_add = None
                __message_add = {}
                __message_add['endpoint'] = self.__ws_endpoint
                __message_add['exchange'] = self.__exchange
                __message_add['symbol'] = self.__ws_symbol
                __message_add['interval'] = self.__interval
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

                if self.__ws_temp_data.full():
                    self.__ws_temp_data.get(True,1)

                self.__ws_temp_data.put(__message_add,True,5)

                __message_out = list(self.__ws_temp_data.queue)
                __message_out.reverse()

                result = __message_out

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

        if __temp_data is not None and isinstance(__temp_data,dict):
            if 'type' in __temp_data and __temp_data['type'] == 'message'\
                and  'data' in __temp_data and isinstance(__temp_data['data'],dict):

                if self.__ws_temp_data is None:
                    self.__ws_temp_data = queue.Queue(maxsize=self.__data_max_len)

                __message_add = None
                __message_add = {}
                __message_add['endpoint'] = self.__ws_endpoint
                __message_add['exchange'] = self.__exchange
                __message_add['symbol'] = self.__ws_symbol
                __message_add['interval'] = self.__interval
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

                if self.__ws_endpoint == 'order_book':
                    __message_out = self.manage_websocket_message_order_book(__temp_data)

                    result = {}
                    result['data'] = __message_out
                    result['min_proc_time_ms'] = 0
                    result['max_proc_time_ms'] = 0

                elif self.__ws_endpoint == 'kline':
                    __message_out = self.manage_websocket_message_kline(__temp_data)

                    result = {}
                    result['data'] = __message_out
                    result['min_proc_time_ms'] = 0
                    result['max_proc_time_ms'] = 0

                elif self.__ws_endpoint == 'trades':
                    __message_out = self.manage_websocket_message_trades(__temp_data)

                    result = {}
                    result['data'] = __message_out
                    result['min_proc_time_ms'] = 0
                    result['max_proc_time_ms'] = 0

                elif self.__ws_endpoint == 'ticker':
                    __message_out = self.manage_websocket_message_ticker(__temp_data)

                    result = {}
                    result['data'] = __message_out
                    result['min_proc_time_ms'] = 0
                    result['max_proc_time_ms'] = 0


        except Exception as exc: # pylint: disable=broad-except
            print(str(exc))

        return result

    def __manage_websocket_diff_data(self,diff_data): # pylint: disable=too-many-branches

        result = False

        current_data = self.__ws_temp_data
        current_data_seq = current_data['last_update_id']

        if isinstance(diff_data['changes'],dict) and 'bids' in diff_data['changes']\
            and 'asks' in diff_data['changes'] and isinstance(diff_data['changes']['bids'],list)\
            and isinstance(diff_data['changes']['asks'],list):
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
            self.__ws_temp_data = current_data
            result = True

        return result
