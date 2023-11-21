"""
Ccxw - CryptoCurrency eXchange Websocket Library
Binance auxiliary functions

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

class BinanceAuxClass(): # pylint: disable=too-many-instance-attributes, duplicate-code
    """
    Ccxw - CryptoCurrency eXchange Websocket Library BinanceAuxClass
    ================================================================
        This class contains helper functions for the Ccxw class.
    """

    # pylint: disable=too-many-arguments
    def __init__(self, endpoint: str=None, symbol: str=None, trading_type: str='SPOT',\
                 testmode: bool=False, api_key: str=None, api_secret: str=None,\
                 result_max_len: int=5, update_speed: str='100ms', interval: str='1m',\
                 data_max_len: int=1000,\
                 debug: bool=False):
        """
        BinanceAuxClass constructor
        ===========================
            Initializes the BinanceAuxClass with the provided parameters.

                :param self: BinanceAuxClass instance.
                :param endpoint: str.
                :param symbol: str unified symbol.
                :param trading_type: str only allowed 'SPOT'.
                :param testmode: bool.
                :param api_key: str Not necessary only for future features.
                :param api_secret: str Not necessary only for future features.
                :param result_max_len: int Max return values > 1 and <= trades_max_len.
                :param update_speed: str only allowed '100ms' | '1000ms' Only for some endpoints.
                :param interval: str only allowed '1m' | '3m' | '5m' | '15m' | '30m' | '1H' | '2H' 
                    | '4H' | '6H' | '8H' | '12H' | '1D' | '3D' | '1W' | '1M'.
                :param data_max_len: int. > 1 and <= 400 max len of data getting from exchange.
                :param debug: bool Verbose output.

                :return: Return a new instance of the Class BinanceAuxClass.
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

        self.__data_max_len = min(self.__data_max_len,400)

        self.__data_max_len = max(self.__data_max_len, 1)

        self.__result_max_len = result_max_len
        self.__update_speed = update_speed
        self.__interval = interval

        self.__result_max_len = min(self.__result_max_len, 500)

        self.__result_max_len = max(self.__result_max_len, 1)

        self.__ws_url_api = None
        self.__ws_url_test = None
        self.__url_api = None
        self.__url_test = None
        self.__ws_endpoint_url = None
        self.__ws_endpoint_on_open_vars = None
        self.__ws_endpoint_on_close_vars = None

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
            self.__ws_url_api = 'wss://stream.binance.com:9443/ws'
            self.__ws_url_test = 'wss://testnet.binance.vision/ws'

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
            self.__url_api = 'https://api.binance.com/api/v3'
            self.__url_test = 'https://testnet.binance.vision/api/v3'

        result = self.__url_api
        if self.__testmode:
            result = self.__url_test

        return result

    def get_exchange_info(self, full_list=False):
        """
        get_exchange_info
        =================
            This function get exchange info. 
                :param full_list: bool.
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

            __l_endpoint = '/exchangeInfo'

            __l_url_point = __l_url_api + __l_endpoint

            if full_list:
                __l_url_point = __l_url_api + __l_endpoint
            else:
                if self.__ws_symbol is not None and isinstance(self.__ws_symbol,str):
                    __l_url_point = __l_url_point + '?symbol='\
                        + str(self.__ws_symbol.replace('/','').upper())

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
            and 'symbols' in __main_data and isinstance(__main_data['symbols'],list):
            result = []
            for symbol_data in __main_data['symbols']:
                if symbol_data is not None and isinstance(symbol_data,dict):
                    if 'baseAsset' in symbol_data and 'quoteAsset' in symbol_data\
                        and isinstance(symbol_data['baseAsset'],str)\
                        and isinstance(symbol_data['quoteAsset'],str):
                        result.append(\
                            symbol_data['baseAsset'].upper() + '/'\
                                + symbol_data['quoteAsset'].upper())

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

        if not self.__update_speed  in ('100ms', '1000ms'):
            self.__update_speed = '1000ms'

        __send_data_vars = None

        if self.__ws_endpoint == 'order_book':
            result = True

            __send_data_vars = None
            __send_data_vars = {}
            __send_data_vars['method'] = 'SUBSCRIBE'
            __send_data_vars['params'] = []
            __send_data_vars['params'].append(self.__ws_symbol.replace("/","").lower() +\
                                              '@depth' + '@' + self.__update_speed)
            __send_data_vars['id'] = int(time.time_ns())

            self.__ws_endpoint_url = ''
            self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)

        elif self.__ws_endpoint == 'kline':
            result = True
            interval_out = self.__interval
            if 'm' in interval_out:
                interval_out = interval_out.replace('m', '')
            elif 'h' in interval_out or 'H' in interval_out:
                interval_out = interval_out.replace('h', '').replace('H','')
                interval_out = 60 * int(interval_out)
            elif 'd' in interval_out or 'D' in interval_out:
                interval_out = 'D'
            elif 'w' in interval_out or 'W' in interval_out:
                interval_out = 'W'
            if 'M' in interval_out:
                interval_out = 'M'

            __send_data_vars = None
            __send_data_vars = {}
            __send_data_vars['method'] = 'SUBSCRIBE'
            __send_data_vars['params'] = []
            __send_data_vars['params'].append(self.__ws_symbol.replace("/","").lower() +\
                                              '@kline_' + str(self.__interval))
            __send_data_vars['id'] = int(time.time_ns())

            self.__ws_endpoint_url = ''
            self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)
        elif self.__ws_endpoint == 'trades':
            result = True

            __send_data_vars = None
            __send_data_vars = {}
            __send_data_vars['method'] = 'SUBSCRIBE'
            __send_data_vars['params'] = []
            __send_data_vars['params'].append(self.__ws_symbol.replace("/","").lower() + '@trade')
            __send_data_vars['id'] = int(time.time_ns())

            self.__ws_endpoint_url = ''
            self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)
        elif self.__ws_endpoint == 'ticker':
            result = True

            __send_data_vars = None
            __send_data_vars = {}
            __send_data_vars['method'] = 'SUBSCRIBE'
            __send_data_vars['params'] = []
            __send_data_vars['params'].append(self.__ws_symbol.replace("/","").lower() + '@ticker')
            __send_data_vars['id'] = int(time.time_ns())

            self.__ws_endpoint_url = ''
            self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)

        if __send_data_vars is not None and isinstance(__send_data_vars,dict):
            self.__ws_endpoint_on_close_vars = __send_data_vars
            self.__ws_endpoint_on_close_vars['method'] = 'UNSUBSCRIBE'
            self.__ws_endpoint_on_close_vars = json.dumps(self.__ws_endpoint_on_close_vars)

        result = {}
        result['ws_endpoint_url'] = self.__ws_endpoint_url
        result['ws_endpoint_on_open_vars'] = self.__ws_endpoint_on_open_vars
        result['ws_endpoint_on_close_vars'] = self.__ws_endpoint_on_close_vars

        return result

    def __init_order_book_data(self):
        """
        __init_order_book_data
        ======================
            This function initialize order book data trought API Call.

                :return bool: Return True if no error ocurred
        """

        result = False

        __url_dest = self.__url_api + '/depth?symbol=' +\
            str(self.__ws_symbol).replace("/","").upper() + '&limit=500'

        __data = ccf.file_get_contents_url(__url_dest)

        if __data is not None and ccf.is_json(__data):
            __data = json.loads(__data)

            self.__ws_temp_data = None
            self.__ws_temp_data = {}
            self.__ws_temp_data['endpoint'] = self.__ws_endpoint
            self.__ws_temp_data['exchange'] = self.__exchange
            self.__ws_temp_data['symbol'] = self.__ws_symbol
            self.__ws_temp_data['interval'] = None
            self.__ws_temp_data['last_update_id'] = __data['lastUpdateId']
            self.__ws_temp_data['diff_update_id'] = 0
            self.__ws_temp_data['bids'] = __data['bids']
            self.__ws_temp_data['asks'] = __data['asks']
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

        if __temp_data is not None and isinstance(__temp_data,dict):
            if 'E' in __temp_data and 'u' in __temp_data\
                and 'U' in __temp_data and 'b' in __temp_data and 'a' in __temp_data:

                if self.__ws_temp_data is None\
                    or (__temp_data['U'] - self.__ws_temp_data['last_update_id']) > 1:
                    if self.__init_order_book_data()\
                        and self.__manage_websocket_diff_data(__temp_data):
                        __proc_data = True
                elif self.__manage_websocket_diff_data(__temp_data):
                    __proc_data = True

        if __proc_data:
            __message_out = None
            __message_out = {}
            __message_out['endpoint'] = self.__ws_endpoint
            __message_out['exchange'] = self.__exchange
            __message_out['symbol'] = self.__ws_symbol
            __message_out['interval'] = None
            __message_out['last_update_id'] = self.__ws_temp_data['last_update_id']
            __message_out['diff_update_id'] = self.__ws_temp_data['diff_update_id']
            __message_out['bids'] = self.__ws_temp_data['bids'][:self.__result_max_len]
            __message_out['asks'] = self.__ws_temp_data['asks'][:self.__result_max_len]
            __message_out['type'] = self.__ws_temp_data['type']
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

        if __temp_data is not None and isinstance(__temp_data,dict)\
            and 'E' in __temp_data and  'k' in __temp_data\
            and isinstance(__temp_data['k'],dict):
            if self.__ws_temp_data is None or not isinstance(self.__ws_temp_data,dict):
                self.__ws_temp_data = {}

            __message_add = None
            __message_add = {}
            __message_add['endpoint'] = self.__ws_endpoint
            __message_add['exchange'] = self.__exchange
            __message_add['symbol'] = self.__ws_symbol
            __message_add['interval'] = self.__interval
            __message_add['last_update_id'] = int(__temp_data['E'])
            __message_add['open_time'] = int(__temp_data['k']['t'])
            __message_add['close_time'] = int(__temp_data['k']['T'])
            __message_add['open_time_date'] = (
                time.strftime("%Y-%m-%d %H:%M:%S",\
                                time.gmtime(int(round(__message_add['open_time']/1000))))
            )

            __message_add['close_time_date'] = (
                time.strftime("%Y-%m-%d %H:%M:%S",\
                                time.gmtime(int(round(__message_add['close_time']/1000))))
            )
            __message_add['open'] = __temp_data['k']['o']
            __message_add['close'] = __temp_data['k']['c']
            __message_add['hight'] = __temp_data['k']['h']
            __message_add['low'] = __temp_data['k']['l']
            __message_add['volume'] = __temp_data['k']['v']
            __message_add['is_closed'] = __temp_data['k']['x']

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
            and 'e' in __temp_data and  'E' in __temp_data:

            if self.__ws_temp_data is None:
                self.__ws_temp_data = queue.Queue(maxsize=self.__data_max_len)

            __message_add = None
            __message_add = {}
            __message_add['endpoint'] = self.__ws_endpoint
            __message_add['exchange'] = self.__exchange
            __message_add['symbol'] = self.__ws_symbol
            __message_add['interval'] = self.__interval
            __message_add['event_time'] = __temp_data['E']
            __message_add['trade_id'] = str(__temp_data['t'])
            __message_add['price'] = str(__temp_data['p'])
            __message_add['quantity'] = str(__temp_data['q'])
            __message_add['trade_time'] = int(__temp_data['T'])
            __message_add['trade_time_date'] = (
                time.strftime("%Y-%m-%d %H:%M:%S",\
                                time.gmtime(\
                                int(round(__message_add['trade_time']/1000)))) + '.' +\
                                str(round(math.modf(\
                                round(__message_add['trade_time']/1000,3))[0]\
                                *1000)).rjust(3,'0')
            )

            __side_of_taker = 'SELL'
            if __temp_data['m']:
                __side_of_taker = 'BUY'
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
            and 'E' in __temp_data and  'e' in __temp_data:
            self.__ws_temp_data = __temp_data

            __message_add = None
            __message_add = {}
            __message_add['endpoint'] = self.__ws_endpoint
            __message_add['exchange'] = self.__exchange
            __message_add['symbol'] = self.__ws_symbol
            __message_add['interval'] = self.__interval
            __message_add['event_type'] =__temp_data['e']
            __message_add['event_time'] = int(__temp_data['E'])
            __gmtime = time.gmtime(int(round(__message_add['event_time']/1000)))
            __message_add['event_time_date'] = (
                time.strftime("%Y-%m-%d %H:%M:%S",__gmtime) + '.' +\
                str(round(math.modf(round(__message_add['event_time']\
                                            /1000,3))[0]*1000)).rjust(3,'0')
            )
            __message_add['price_change'] = __temp_data['p']
            __message_add['price_change_percent'] = __temp_data['P']
            __message_add['weighted_average_price'] = __temp_data['w']
            __message_add['first_trade_before_the_24hr_rolling_window'] = (
                __temp_data['x']
            )
            __message_add['last_price'] = __temp_data['c']
            __message_add['last_quantity'] = __temp_data['Q']
            __message_add['best_bid_price'] = __temp_data['b']
            __message_add['best_bid_quantity'] = __temp_data['B']
            __message_add['best_ask_price'] = __temp_data['a']
            __message_add['best_ask_quantity'] = __temp_data['A']
            __message_add['open_price'] = __temp_data['o']
            __message_add['high_price'] = __temp_data['h']
            __message_add['low_price'] = __temp_data['l']
            __message_add['total_traded_base_asset_volume'] = __temp_data['v']
            __message_add['total_traded_quote_asset_volume'] = __temp_data['q']
            __message_add['statistics_open_time'] = __temp_data['O']
            __statistics_open_time = (
                time.strftime("%Y-%m-%d %H:%M:%S",\
                                time.gmtime(int(round(\
                                    __message_add['statistics_open_time']/1000))))
            )
            __message_add['statistics_open_time_date'] = (
                __statistics_open_time + '.' +\
                    str(round(math.modf(round(__message_add['statistics_open_time']\
                                                /1000,3))[0]*1000)).rjust(3,'0')
            )
            __message_add['statistics_close_time'] = __temp_data['C']
            __statistics_close_time = (
                time.gmtime(int(round(__message_add['statistics_close_time']/1000)))
            )
            __message_add['statistics_close_time_date'] = (
                time.strftime("%Y-%m-%d %H:%M:%S",__statistics_close_time)\
                    + '.' + str(round(math.modf(round(\
                        __message_add['statistics_close_time']/1000,\
                            3))[0]*1000)).rjust(3,'0')
            )
            __message_add['total_number_of_trades'] = __temp_data['n']

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

                    if __message_out is not None:
                        result = {}
                        result['data'] = __message_out
                        result['min_proc_time_ms'] = 0
                        result['max_proc_time_ms'] = 0

                elif self.__ws_endpoint == 'kline':
                    __message_out = self.manage_websocket_message_kline(__temp_data)

                    if __message_out is not None:
                        result = {}
                        result['data'] = __message_out[:self.__result_max_len]
                        result['min_proc_time_ms'] = 0
                        result['max_proc_time_ms'] = 0

                elif self.__ws_endpoint == 'trades':
                    __message_out = self.manage_websocket_message_trades(__temp_data)

                    if __message_out is not None:
                        result = {}
                        result['data'] = __message_out[:self.__result_max_len]
                        result['min_proc_time_ms'] = 0
                        result['max_proc_time_ms'] = 0

                elif self.__ws_endpoint == 'ticker':
                    __message_out = self.manage_websocket_message_ticker(__temp_data)

                    if __message_out is not None:
                        result = {}
                        result['data'] = __message_out
                        result['min_proc_time_ms'] = 0
                        result['max_proc_time_ms'] = 0



        except Exception as exc: # pylint: disable=broad-except
            print(str(exc))

        return result

    def __manage_websocket_diff_data(self,diff_data):
        """
        __manage_websocket_diff_data
        ============================
            This function manage websocket message and normalize result data.

                :param diff_data: dict websocket message data.
                
                :return bool: Return True if no error ocurred.
        """

        result = False

        current_data = self.__ws_temp_data

        if isinstance(diff_data,dict) and 'u' in diff_data and 'b' in diff_data\
            and 'a' in diff_data:
            if isinstance(diff_data['b'],list) and isinstance(diff_data['a'],list):
                __temp_bids_d = {}
                __temp_asks_d = {}
                __temp_bids_l = []
                __temp_asks_l = []

                __key_c = 'bids'
                __key_d = 'b'
                for i in range(0,len(current_data[__key_c])):
                    __temp_bids_d[current_data[__key_c][i][0]] = current_data[__key_c][i][1]

                for i in range(0,len(diff_data[__key_d])):
                    if float(diff_data[__key_d][i][1]) == 0:
                        n_t = __temp_bids_d.pop(diff_data[__key_d][i][0],None) # pylint: disable=unused-variable
                    else:
                        __temp_bids_d[diff_data[__key_d][i][0]] = diff_data[__key_d][i][1]

                for i,j in __temp_bids_d.items():
                    __temp_bids_l.append([i,j])

                __temp_bids = sorted(__temp_bids_l, key=lambda value: float(value[0]), reverse=True)

                __key_c = 'asks'
                __key_d = 'a'
                for i in range(0,len(current_data[__key_c])):
                    __temp_asks_d[current_data[__key_c][i][0]] = current_data[__key_c][i][1]

                for i in range(0,len(diff_data[__key_d])):
                    if float(diff_data[__key_d][i][1]) == 0:
                        n_t = __temp_asks_d.pop(diff_data[__key_d][i][0],None)
                    else:
                        __temp_asks_d[diff_data[__key_d][i][0]] = diff_data[__key_d][i][1]

                for i,j in __temp_asks_d.items():
                    __temp_asks_l.append([i,j])

                __temp_asks = (
                    sorted(__temp_asks_l, key=lambda value: float(value[0]), reverse=False)
                )

                current_data['bids'] = __temp_bids
                current_data['asks'] = __temp_asks
                current_data['diff_update_id'] = diff_data['U'] - current_data['last_update_id']
                current_data['last_update_id'] = diff_data['u']
                current_data['type'] = 'update'
                self.__ws_temp_data = current_data
                result = True

        return result
