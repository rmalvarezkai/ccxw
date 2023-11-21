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

import ccxw.ccxw_common_functions as ccf


class OkxAuxClass(): # pylint: disable=too-many-instance-attributes, duplicate-code
    """
    Ccxw - CryptoCurrency eXchange Websocket Library OkxAuxClass
    ================================================================
        This class contains helper functions for the Ccxw class
    """

    # pylint: disable=too-many-arguments
    def __init__(self, endpoint: str=None, symbol: str=None, trading_type: str='SPOT',\
                 testmode: bool=False, api_key: str=None, api_secret: str=None,\
                 result_max_len: int=5, update_speed: str='100ms', interval: str='1m',\
                 data_max_len: int=1000, debug: bool=False):
        """
        OkxAuxClass constructor
        =======================
            This class contains helper functions for the Ccxw class.

                :param self: OkxAuxClass instance.
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

                :return: Return a new instance of the Class OkxAuxClass.
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

        if self.__trading_type == 'SPOT':
            self.__ws_url_api = 'wss://ws.okx.com:8443/ws/v5'
            self.__ws_url_test = 'wss://wspap.okx.com:8443/ws/v5'

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
            self.__url_api = 'https://www.okx.com'
            self.__url_test = 'https://www.okx.com'

        result = self.__url_api
        if self.__testmode:
            result = self.__url_test

        return result

    def get_exchange_info(self, full_list=False):
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

            __l_endpoint = '/api/v5/public/instruments?instType=' + self.__trading_type.upper()

            __l_url_point = __l_url_api + __l_endpoint

            if full_list:
                __l_url_point = __l_url_api + __l_endpoint
            else:
                if self.__ws_symbol is not None and isinstance(self.__ws_symbol,str):
                    __l_url_point = (
                        __l_url_point + '&instId=' + str(self.__ws_symbol.replace('/','-').upper())
                    )

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
        __main_data = self.get_exchange_info(True)

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
            __send_data_vars['op'] = 'subscribe'
            __send_data_vars['args'] = []

            __data_node = {}
            __data_node['channel'] = 'books'
            __data_node['instId'] = self.__ws_symbol.replace("/","-").upper()
            __send_data_vars['args'].append(__data_node)

            self.__ws_endpoint_url = '/public'
            if self.__testmode:
                self.__ws_endpoint_url = '/public'

            self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)

        elif self.__ws_endpoint == 'kline':

            interval_out = 'candle' + self.__interval

            __send_data_vars = None
            __send_data_vars = {}
            __send_data_vars['op'] = 'subscribe'
            __send_data_vars['args'] = []

            __data_node = {}
            __data_node['channel'] = interval_out
            __data_node['instId'] = self.__ws_symbol.replace("/","-").upper()
            __send_data_vars['args'].append(__data_node)

            self.__ws_endpoint_url = '/business'
            if self.__testmode:
                self.__ws_endpoint_url = '/business?brokerId=9999'
            self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)

        elif self.__ws_endpoint == 'trades':

            __send_data_vars = None
            __send_data_vars = {}
            __send_data_vars['op'] = 'subscribe'
            __send_data_vars['args'] = []

            __data_node = {}
            __data_node['channel'] = 'trades'
            __data_node['instId'] = self.__ws_symbol.replace("/","-").upper()
            __send_data_vars['args'].append(__data_node)

            self.__ws_endpoint_url = '/business'
            if self.__testmode:
                self.__ws_endpoint_url = '/business?brokerId=9999'

            self.__ws_endpoint_url = '/public'
            if self.__testmode:
                self.__ws_endpoint_url = '/public'

            self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)

        elif self.__ws_endpoint == 'ticker':

            __send_data_vars = None
            __send_data_vars = {}
            __send_data_vars['op'] = 'subscribe'
            __send_data_vars['args'] = []

            __data_node = {}
            __data_node['channel'] = 'tickers'
            __data_node['instId'] = self.__ws_symbol.replace("/","-").upper()
            __send_data_vars['args'].append(__data_node)

            self.__ws_endpoint_url = '/business'
            if self.__testmode:
                self.__ws_endpoint_url = '/business?brokerId=9999'

            self.__ws_endpoint_url = '/public'
            if self.__testmode:
                self.__ws_endpoint_url = '/public'

            self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)

        if __send_data_vars is not None and isinstance(__send_data_vars,dict):
            self.__ws_endpoint_on_close_vars = __send_data_vars
            self.__ws_endpoint_on_close_vars['op'] = 'unsubscribe'
            self.__ws_endpoint_on_close_vars = json.dumps(self.__ws_endpoint_on_close_vars)

        result = {}
        result['ws_endpoint_url'] = self.__ws_endpoint_url
        result['ws_endpoint_on_open_vars'] = self.__ws_endpoint_on_open_vars
        result['ws_endpoint_on_close_vars'] = self.__ws_endpoint_on_close_vars


        return result

    def __init_order_book_data(self,temp_data):

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

                    self.__ws_temp_data = __data_out

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
        __diff_update_id = 0
        __data_type = 'snapshot'

        if __temp_data is not None and isinstance(__temp_data,dict)\
            and 'action' in __temp_data:

            if __temp_data['action'] == 'snapshot':
                if self.__init_order_book_data(__temp_data):
                    __proc_data = True
            elif __temp_data['action'] == 'update' and self.__ws_temp_data is not None\
                and isinstance(self.__ws_temp_data,dict):
                __diff_update_id = (
                    __temp_data['data'][0]['seqId']\
                        - self.__ws_temp_data['data'][0]['seqId']
                )
                if self.__manage_websocket_diff_data(__temp_data):
                    __data_type = 'update'
                    __proc_data = True

            if __proc_data:
                __message_out = None
                __message_out = {}
                __message_out['endpoint'] = self.__ws_endpoint
                __message_out['exchange'] = self.__exchange
                __message_out['symbol'] = self.__ws_symbol
                __message_out['interval'] = None
                __message_out['last_update_id'] = __temp_data['data'][0]['seqId']
                __message_out['diff_update_id'] = __diff_update_id
                __message_out['bids'] = (
                    self.__ws_temp_data['data'][0]['bids'][:self.__result_max_len]
                )
                __message_out['asks'] = (
                    self.__ws_temp_data['data'][0]['asks'][:self.__result_max_len]
                )
                __message_out['type'] = __data_type
                __current_datetime = datetime.datetime.utcnow()
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

        if __temp_data is not None and isinstance(__temp_data,dict)\
            and 'data' in __temp_data and isinstance(__temp_data['data'],list):
            if len(__temp_data['data']) > 0\
                and isinstance(__temp_data['data'][0],list)\
                and len(__temp_data['data'][0]) >= 9:

                if self.__ws_temp_data is None or not isinstance(self.__ws_temp_data,dict):
                    self.__ws_temp_data = {}

                for i in range(0,len(__temp_data['data'])):
                    __is_confirmed = False
                    if int(__temp_data['data'][i][8]) == 1:
                        __is_confirmed = True

                    __message_add = None
                    __message_add = {}
                    __message_add['endpoint'] = self.__ws_endpoint
                    __message_add['exchange'] = self.__exchange
                    __message_add['symbol'] = self.__ws_symbol
                    __message_add['interval'] = self.__interval
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

        if __temp_data is not None and isinstance(__temp_data,dict)\
            and 'data' in __temp_data\
            and isinstance(__temp_data['data'],list) and len(__temp_data['data']) > 0:

            if self.__ws_temp_data is None:
                self.__ws_temp_data = queue.Queue(maxsize=self.__data_max_len)

            for i in range(len(__temp_data['data']) - 1,-1,-1):
                __message_add = None
                __message_add = {}
                __message_add['endpoint'] = self.__ws_endpoint
                __message_add['exchange'] = self.__exchange
                __message_add['symbol'] = self.__ws_symbol
                __message_add['interval'] = self.__interval
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

        if __temp_data is not None and isinstance(__temp_data,dict)\
            and 'data' in __temp_data and isinstance(__temp_data['data'],list)\
            and len(__temp_data['data']) > 0:

            self.__ws_temp_data = __temp_data

            i = 0

            __message_add = None
            __message_add = {}
            __message_add['endpoint'] = self.__ws_endpoint
            __message_add['exchange'] = self.__exchange
            __message_add['symbol'] = self.__ws_symbol
            __message_add['interval'] = self.__interval
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

    def __manage_websocket_diff_data(self,diff_data): # pylint: disable=too-many-locals
        result = False

        current_data = self.__ws_temp_data

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
            self.__ws_temp_data = current_data

            result = True

        return result
