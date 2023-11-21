"""
Ccxw - CryptoCurrency eXchange Websocket Library
ByBit auxiliary functions

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

class BybitAuxClass(): # pylint: disable=too-many-instance-attributes
    """
    Ccxw - CryptoCurrency eXchange Websocket Library BybitAuxClass
    ==============================================================
        This class contains helper functions for the Ccxw class.
    """

    # pylint: disable=too-many-arguments
    def __init__(self, endpoint: str=None, symbol: str=None, trading_type: str='SPOT',\
                 testmode: bool=False, api_key: str=None, api_secret: str=None,\
                 result_max_len: int=5, update_speed: str='100ms', interval: str='1m',\
                 data_max_len: int=1000, debug: bool=False):
        """
        BybitAuxClass constructor
        =========================
            Initializes the BybitAuxClass with the provided parameters.

                :param self: BybitAuxClass instance.
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

                :return: Return a new instance of the Class BybitAuxClass.
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

        self.__api_type = 'public' # pylint: disable=unused-private-member

        self.__exchange_hostname = None # pylint: disable=unused-private-member
        self.__ws_url_api = None
        self.__ws_url_test = None
        self.__ws_url = None # pylint: disable=unused-private-member
        self.__url_api = None
        self.__url_test = None
        self.__ws_endpoint_url = None
        self.__ws_endpoint_on_open_vars = None
        self.__ws_endpoint_on_close_vars = None
        self.__ws_endpoint_on_auth_vars = None
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

        self.__ws_url_api = 'wss://stream.bybit.com/v5/public/' + self.__trading_type.lower()
        self.__ws_url_test = 'wss://stream-testnet.bybit.com/v5/public/'\
            + self.__trading_type.lower()

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
            self.__url_api = 'https://api.bybit.com'
            self.__url_test = 'https://api-testnet.bybit.com'

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

            __l_endpoint = '/v5/market/instruments-info?category=' + self.__trading_type.lower()

            __l_url_point = __l_url_api + __l_endpoint

            if full_list:
                __l_url_point = __l_url_api + __l_endpoint
            else:
                if self.__ws_symbol is not None and isinstance(self.__ws_symbol,str):
                    __l_url_point = __l_url_point + '&symbol='\
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

        if __main_data is not None and isinstance(__main_data,dict):
            if 'result' in __main_data and isinstance(__main_data['result'],dict)\
                and 'list' in __main_data['result']\
                and isinstance(__main_data['result']['list'],list):

                result = []
                for symbol_data in __main_data['result']['list']:
                    if symbol_data is not None and isinstance(symbol_data,dict):
                        if 'baseCoin' in symbol_data and 'quoteCoin' in symbol_data\
                            and isinstance(symbol_data['baseCoin'],str)\
                            and isinstance(symbol_data['quoteCoin'],str):
                            result.append(\
                                symbol_data['baseCoin'].upper() + '/'\
                                + symbol_data['quoteCoin'].upper())

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

    def get_websocket_endpoint_path(self): # pylint: disable=too-many-statements, duplicate-code
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
            result = True

            if self.__update_speed != '100ms' or self.__update_speed != '1000ms':
                self.__update_speed = '1000ms'

            __send_data_vars = None
            __send_data_vars = {}
            __send_data_vars['op'] = 'subscribe'
            __send_data_vars['args'] = []
            __send_data_vars['args'].append('orderbook.' + str(50) + '.'\
                                            + self.__ws_symbol.replace("/","").upper())
            __send_data_vars['req_id'] = str(time.time_ns())

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
            __send_data_vars['op'] = 'subscribe'
            __send_data_vars['args'] = []
            __send_data_vars['args'].append('kline.' + str(interval_out)\
                                            + '.' + self.__ws_symbol.replace("/","").upper())
            __send_data_vars['req_id'] = str(time.time_ns())

            self.__ws_endpoint_url = ''
            self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)
        elif self.__ws_endpoint == 'trades':
            result = True

            __send_data_vars = None
            __send_data_vars = {}
            __send_data_vars['op'] = 'subscribe'
            __send_data_vars['args'] = []
            __send_data_vars['args'].append('publicTrade' + '.'\
                                            + self.__ws_symbol.replace("/","").upper())
            __send_data_vars['req_id'] = str(time.time_ns())

            self.__ws_endpoint_url = ''
            self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)

        elif self.__ws_endpoint == 'ticker':
            result = True

            __send_data_vars = None
            __send_data_vars = {}
            __send_data_vars['op'] = 'subscribe'
            __send_data_vars['args'] = []
            __send_data_vars['args'].append('tickers' + '.'\
                                            + self.__ws_symbol.replace("/","").upper())
            __send_data_vars['req_id'] = str(time.time_ns())

            self.__ws_endpoint_url = ''
            self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)

        if __send_data_vars is not None and isinstance(__send_data_vars,dict):
            self.__ws_endpoint_on_close_vars = __send_data_vars
            self.__ws_endpoint_on_close_vars['op'] = 'unsubscribe'
            self.__ws_endpoint_on_close_vars = json.dumps(self.__ws_endpoint_on_close_vars)

        result = {}
        result['ws_endpoint_url'] = self.__ws_endpoint_url
        result['ws_endpoint_on_auth_vars'] = self.__ws_endpoint_on_auth_vars
        result['ws_endpoint_on_open_vars'] = self.__ws_endpoint_on_open_vars
        result['ws_endpoint_on_close_vars'] = self.__ws_endpoint_on_close_vars

        return result

    def __init_order_book_data(self,temp_data):

        result = False
        self.__ws_temp_data = temp_data
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
            and 'type' in __temp_data:
            if self.__ws_temp_data is None or __temp_data['type'] == 'snapshot':
                if self.__init_order_book_data(__temp_data):
                    __proc_data = True
            elif self.__manage_websocket_diff_data(__temp_data):
                __diff_update_id = (
                    __temp_data['data']['u'] - self.__ws_temp_data['data']['u']
                )
                __data_type = 'update'
                __proc_data = True

        if __proc_data:
            __message_out = None
            __message_out = {}
            __message_out['endpoint'] = self.__ws_endpoint
            __message_out['exchange'] = self.__exchange
            __message_out['symbol'] = self.__ws_symbol
            __message_out['interval'] = None
            __message_out['last_update_id'] = self.__ws_temp_data['data']['u']
            __message_out['diff_update_id'] = __diff_update_id
            __message_out['bids'] = (
                self.__ws_temp_data['data']['b'][:self.__result_max_len]
            )
            __message_out['asks'] = (
                self.__ws_temp_data['data']['a'][:self.__result_max_len]
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

        if __temp_data is not None and isinstance(__temp_data,dict):
            if 'type' in __temp_data and 'data' in __temp_data\
                and isinstance(__temp_data['data'],list) and len(__temp_data['data']) > 0:
                if self.__ws_temp_data is None or not isinstance(self.__ws_temp_data,dict):
                    self.__ws_temp_data = {}

                for i in range(0,len(__temp_data['data'])):
                    __message_add = None
                    __message_add = {}
                    __message_add['endpoint'] = self.__ws_endpoint
                    __message_add['exchange'] = self.__exchange
                    __message_add['symbol'] = self.__ws_symbol
                    __message_add['interval'] = self.__interval
                    __message_add['last_update_id'] = int(__temp_data['ts'])
                    __message_add['open_time'] = int(__temp_data['data'][i]['start'])
                    __message_add['close_time'] = int(__temp_data['data'][i]['end'])
                    __message_add['open_time_date'] = (
                        time.strftime("%Y-%m-%d %H:%M:%S",\
                                        time.gmtime(int(\
                                            round(__message_add['open_time']/1000))))
                    )
                    __message_add['close_time_date'] = (
                        time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(int(\
                            round(__message_add['close_time']/1000))))
                    )
                    __message_add['open'] = __temp_data['data'][i]['open']
                    __message_add['close'] = __temp_data['data'][i]['close']
                    __message_add['hight'] = __temp_data['data'][i]['high']
                    __message_add['low'] = __temp_data['data'][i]['low']
                    __message_add['volume'] = __temp_data['data'][i]['volume']
                    __message_add['is_closed'] = __temp_data['data'][i]['confirm']

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
            if 'ts' in __temp_data and 'data' in __temp_data\
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
                    __message_add['event_time'] = __temp_data['ts']
                    __message_add['trade_id'] = str(__temp_data['data'][i]['i'])
                    __message_add['price'] = str(__temp_data['data'][i]['p'])
                    __message_add['quantity'] = str(__temp_data['data'][i]['v'])
                    __message_add['trade_time'] = __temp_data['data'][i]['T']
                    __message_add['trade_time_date'] = (
                        time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(int(\
                            round(__message_add['trade_time']/1000))))\
                            + '.' + str(round(math.modf(\
                                round(__message_add['trade_time']\
                                        /1000,3))[0]*1000)).rjust(3,'0')
                    )
                    __message_add['side_of_taker'] = __temp_data['data'][i]['S'].upper()

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
            if 'cs' in __temp_data and 'data' in __temp_data\
                and isinstance(__temp_data['data'],dict) and len(__temp_data['data']) > 0:

                self.__ws_temp_data = __temp_data

                __message_add = None
                __message_add = {}
                __message_add['endpoint'] = self.__ws_endpoint
                __message_add['exchange'] = self.__exchange
                __message_add['symbol'] = self.__ws_symbol
                __message_add['interval'] = self.__interval
                __message_add['event_type'] = '24hrTicker'
                __message_add['event_time'] = __temp_data['cs']
                __message_add['event_time_date'] = (
                    time.strftime("%Y-%m-%d %H:%M:%S",\
                                    time.gmtime(int(round(__message_add['event_time']\
                                                        /1000)))) + '.'\
                        + str(round(math.modf(round(__message_add['event_time']/1000,\
                                                    3))[0]*1000)).rjust(3,'0')
                )
                __message_add['price_change'] = (
                    str(round(float(__temp_data['data']['price24hPcnt'])\
                                * float(__temp_data['data']['lastPrice']),8))
                )
                __message_add['price_change_percent'] = __temp_data['data']['price24hPcnt']
                __message_add['weighted_average_price'] = None
                __message_add['first_trade_before_the_24hr_rolling_window'] = None
                __message_add['last_price'] = __temp_data['data']['lastPrice']
                __message_add['last_quantity'] = None
                __message_add['best_bid_price'] = None
                __message_add['best_bid_quantity'] = None
                __message_add['best_ask_price'] = None
                __message_add['best_ask_quantity'] = None
                __message_add['open_price'] = __temp_data['data']['prevPrice24h']
                __message_add['high_price'] = __temp_data['data']['highPrice24h']
                __message_add['low_price'] = __temp_data['data']['lowPrice24h']
                __message_add['total_traded_base_asset_volume'] = (
                    __temp_data['data']['volume24h']
                )
                __message_add['total_traded_quote_asset_volume'] = (
                    __temp_data['data']['turnover24h']
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

        if isinstance(diff_data,dict) and 'ts' in diff_data and 'data' in diff_data\
            and 'u' in diff_data['data'] and 'seq' in diff_data['data']:
            if 'b' in diff_data['data'] and 'a' in diff_data['data']\
                and isinstance(diff_data['data']['b'],list)\
                and isinstance(diff_data['data']['a'],list):
                __temp_bids_d = {}
                __temp_asks_d = {}
                __temp_bids_l = []
                __temp_asks_l = []

                __key = 'b'
                for i in range(0,len(current_data['data'][__key])):
                    __temp_bids_d[current_data['data'][__key][i][0]] = (
                        current_data['data'][__key][i][1]
                    )

                for i in range(0,len(diff_data['data'][__key])):
                    if float(diff_data['data'][__key][i][1]) == 0:
                        n_t = __temp_bids_d.pop(diff_data['data'][__key][i][0],None) # pylint: disable=unused-variable
                    else:
                        __temp_bids_d[diff_data['data'][__key][i][0]] = (
                            diff_data['data'][__key][i][1]
                        )

                for i,j in __temp_bids_d.items():
                    __temp_bids_l.append([i,j])

                __temp_bids = sorted(__temp_bids_l, key=lambda value: float(value[0]), reverse=True)

                __key = 'a'
                for i in range(0,len(current_data['data'][__key])):
                    __temp_asks_d[current_data['data'][__key][i][0]] = (
                        current_data['data'][__key][i][1]
                    )

                for i in range(0,len(diff_data['data'][__key])):
                    if float(diff_data['data'][__key][i][1]) == 0:
                        n_t = __temp_asks_d.pop(diff_data['data'][__key][i][0],None)
                    else:
                        __temp_asks_d[diff_data['data'][__key][i][0]] = (
                            diff_data['data'][__key][i][1]
                        )

                for i,j in __temp_asks_d.items():
                    __temp_asks_l.append([i,j])

                __temp_asks = (
                    sorted(__temp_asks_l, key=lambda value: float(value[0]), reverse=False)
                )

                current_data['data']['b'] = __temp_bids
                current_data['data']['a'] = __temp_asks
                current_data['data']['u'] = diff_data['data']['u']
                current_data['data']['seq'] = diff_data['data']['seq']
                self.__ws_temp_data = current_data
                result = True

        return result
