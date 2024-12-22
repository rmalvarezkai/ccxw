"""
Ccxw - CryptoCurrency eXchange Websocket Library
Binanceus auxiliary functions

Author: Ricardo Marcelo Alvarez
Date: 2023-10-31
"""

import os
import json
import time
import datetime
import queue
import math
import pprint # pylint: disable=unused-import
import ccxw.ccxw_common_functions as ccf
from ccxw.safe_thread_vars import DictSafeThread
import ccxw

class BinanceusCcxwAuxClass():
    """
    Ccxw - CryptoCurrency eXchange Websocket Library BinanceusCcxwAuxClass
    ======================================================================
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
        BinanceusCcxwAuxClass constructor
        =================================
            :param self: BinanceusCcxwAuxClass instance.
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
                | '4H' | '6H' | '8H' | '12H' | '1D' | '3D' | '1W' | '1M'.
            :param data_max_len: int. > 1 and <= 2500 max len of data getting from exchange.
            :param debug: bool Verbose output.

            :return: Return a new instance of the Class BinanceusCcxwAuxClass.
        """

        __exchange_limit_streams = 1024

        self.__exchange = os.path.basename(__file__)[:-3]
        self.__ws_streams = streams
        self.__testmode = testmode

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
        self.__url_api = None
        self.__url_test = None
        self.__ws_endpoint_url = None
        self.__ws_endpoint_on_open_vars = None
        self.__ws_endpoint_on_close_vars = None

        self.__ws_temp_data = DictSafeThread()

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

    def get_websocket_url(self):
        """
        get_websocket_url
        =================
            This function set and return websocket URL.

                :return str: Return websocket URL.
        """

        result = None
        if self.__trading_type.upper() == 'SPOT':
            self.__ws_url_api = 'wss://stream.binance.us:9443/ws'
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

        if self.__trading_type.upper() == 'SPOT':
            self.__url_api = 'https://api.binance.us/api/v3'
            self.__url_test = 'https://testnet.binance.vision/api/v3'

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
            __l_endpoint = '/exchangeInfo'

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
            if symbol.replace('/', '').lower() == symbol_rpl.replace('/', '').lower():
                result = symbol_rpl
                break

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
            result = str(result)

        return result

    def if_symbol_supported(self, symbol):
        """
        if_symbol_supported
        ===================
            This function check if symbol is supported by the exchange.
                :param symbol: str unified symbol.
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

        __send_data_vars = None
        __send_data_vars = {}
        __send_data_vars['method'] = 'SUBSCRIBE'
        __send_data_vars['params'] = []
        __send_data_vars['id'] = int(time.time_ns())

        self.__ws_endpoint_url = ''

        for stream in self.__ws_streams:
            interval = 'none'
            update_speed = '100ms'

            if stream['endpoint'] == 'order_book':
                result = True

                __send_data_vars['params'].append(stream['symbol'].replace("/","").lower() +\
                                                '@depth' + '@' + update_speed)

            elif stream['endpoint'] == 'kline':
                result = True
                interval = stream['interval']
                interval_out = str(interval)
                if 'm' in interval_out:
                    interval_out = interval_out.replace('m', '')
                elif 'h' in interval_out or 'H' in interval_out:
                    interval_out = interval_out.replace('h', '').replace('H','')
                    interval_out = 60 * int(interval_out)
                elif 'd' in interval_out or 'D' in interval_out:
                    interval_out = 'D'
                elif 'w' in interval_out or 'W' in interval_out:
                    interval_out = 'W'
                elif 'M' in interval_out:
                    interval_out = 'M'

                __send_data_vars['params'].append(stream['symbol'].replace("/","").lower() +\
                                                  '@kline_' + str(interval))

            elif stream['endpoint'] == 'trades':
                result = True
                __send_data_vars['params'].append(stream['symbol'].replace("/","").lower() +\
                                                  '@trade')

            elif stream['endpoint'] == 'ticker':
                result = True
                __send_data_vars['params'].append(stream['symbol'].replace("/","").lower() +\
                                                  '@ticker')

            __stream_index = self.get_stream_index(stream['endpoint'],\
                                                     stream['symbol'],\
                                                     interval=interval)
            self.__ws_temp_data[__stream_index] = None

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

    # Desde aca tengo que cambiar el codigo de los otros exchanges

    def __init_order_book_data(self,\
                               endpoint,\
                               symbol,\
                               interval: str='none'):
        """
        __init_order_book_data
        ======================
            This function initialize order book data trought API Call.
                :param self: This class instance.
                :param endpoint: str
                :param symbol: str
                :param interval: str
                :return bool: Return True if order book is initialized
        """

        result = False

        __stream_index = self.get_stream_index(endpoint, symbol, interval=interval)

        __url_dest = self.__url_api + '/depth?symbol=' +\
            str(symbol).replace("/","").upper() + '&limit=1000'

        __data = ccf.file_get_contents_url(__url_dest)

        if __data is not None and ccf.is_json(__data):
            __data = json.loads(__data)

            self.__ws_temp_data[__stream_index] = {}
            self.__ws_temp_data[__stream_index]['endpoint'] = endpoint
            self.__ws_temp_data[__stream_index]['exchange'] = self.__exchange
            self.__ws_temp_data[__stream_index]['symbol'] = symbol
            self.__ws_temp_data[__stream_index]['interval'] = None
            self.__ws_temp_data[__stream_index]['last_update_id'] = __data['lastUpdateId']
            self.__ws_temp_data[__stream_index]['diff_update_id'] = 0
            self.__ws_temp_data[__stream_index]['bids'] = __data['bids']
            self.__ws_temp_data[__stream_index]['asks'] = __data['asks']
            self.__ws_temp_data[__stream_index]['type'] = 'snapshot'

            result = True
        else:
            result = False

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
        __stream_index = None

        if __temp_data is not None and isinstance(__temp_data, dict):
            if 'E' in __temp_data and 'u' in __temp_data\
                and 's' in __temp_data and 'U' in __temp_data\
                and 'b' in __temp_data and 'a' in __temp_data:
                __stream_index = self.get_stream_index('order_book', __temp_data['s'])

                __diff_data = 0

                if self.__ws_temp_data[__stream_index] is not None\
                    and isinstance(self.__ws_temp_data[__stream_index], dict)\
                    and 'last_update_id' in self.__ws_temp_data[__stream_index]:
                    __diff_data = __temp_data['U']\
                        - self.__ws_temp_data[__stream_index]['last_update_id']

                if self.__ws_temp_data[__stream_index] is None or __diff_data > 1:
                    if self.__init_order_book_data('order_book', str(__temp_data['s']))\
                        and self.__manage_websocket_diff_data(__temp_data):
                        __proc_data = True
                elif self.__manage_websocket_diff_data(__temp_data):
                    __proc_data = True


        if __proc_data:

            __message_out = None
            __message_out = {}
            __message_out['endpoint'] = 'order_book'
            __message_out['exchange'] = self.__exchange
            __message_out['symbol'] = __temp_data['s']
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
            __current_datetime = datetime.datetime.now(datetime.timezone.utc)
            __current_timestamp = __current_datetime.strftime("%s.%f")
            __current_datetime = __current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
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


        if __temp_data is not None and isinstance(__temp_data,dict)\
            and 'E' in __temp_data and  'k' in __temp_data\
            and isinstance(__temp_data['k'],dict)\
            and 's' in __temp_data['k']\
            and 'i' in __temp_data['k']:

            __stream_index = self.get_stream_index('kline',\
                                                   __temp_data['k']['s'],\
                                                   __temp_data['k']['i'])
            if self.__ws_temp_data[__stream_index] is None\
                or not isinstance(self.__ws_temp_data[__stream_index],dict):
                self.__ws_temp_data[__stream_index] = {}

            __message_add = None
            __message_add = {}
            __message_add['endpoint'] = 'kline'
            __message_add['exchange'] = self.__exchange
            __message_add['symbol'] = __temp_data['k']['s']
            __message_add['interval'] = __temp_data['k']['i']
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


            self.__ws_temp_data[__stream_index][int(__message_add['open_time'])] = __message_add

            while len(self.__ws_temp_data[__stream_index]) > self.__data_max_len:
                __first_key = min(list(self.__ws_temp_data[__stream_index].keys()))
                __nc = self.__ws_temp_data[__stream_index].pop(__first_key,None)

            __message_out = list(self.__ws_temp_data[__stream_index].values())
            result = __message_out[:self.__result_max_len]

        return result

    def manage_websocket_message_trades(self, data):
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
            and 'e' in __temp_data and  'E' in __temp_data\
            and 's' in __temp_data:

            __stream_index = self.get_stream_index('trades', __temp_data['s'])


            if self.__ws_temp_data[__stream_index] is None:
                self.__ws_temp_data[__stream_index] = queue.Queue(maxsize=self.__data_max_len)

            __message_add = None
            __message_add = {}
            __message_add['endpoint'] = 'trades'
            __message_add['exchange'] = self.__exchange
            __message_add['symbol'] = __temp_data['s']
            __message_add['interval'] = None
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

            if self.__ws_temp_data[__stream_index].full():
                self.__ws_temp_data[__stream_index].get(True,1)

            self.__ws_temp_data[__stream_index].put(__message_add,True,5)

            __message_out = list(self.__ws_temp_data[__stream_index].queue)
            #__message_out.reverse()

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

        if __temp_data is not None and isinstance(__temp_data,dict)\
            and 'E' in __temp_data and  'e' in __temp_data\
            and 's' in __temp_data:
            __stream_index = self.get_stream_index('ticker', __temp_data['s'])

            self.__ws_temp_data[__stream_index] = __temp_data

            __message_add = None
            __message_add = {}
            __message_add['endpoint'] = 'ticker'
            __message_add['exchange'] = self.__exchange
            __message_add['symbol'] = __temp_data['s']
            __message_add['interval'] = None
            __message_add['event_type'] = __temp_data['e']
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
            if ccf.is_json(message_in):
                __temp_data = json.loads(message_in)

                __endpoint = 'NONE'
                if isinstance(__temp_data, dict):
                    if 'e' in __temp_data:
                        if __temp_data['e'] == 'depthUpdate':
                            __endpoint = 'order_book'
                        elif __temp_data['e'] == 'kline':
                            __endpoint = 'kline'
                        elif __temp_data['e'] == 'trade':
                            __endpoint = 'trades'
                        elif __temp_data['e'] == '24hrTicker':
                            __endpoint = 'ticker'

                if __endpoint == 'order_book':
                    __message_out = self.manage_websocket_message_order_book(__temp_data)

                    if __message_out is not None:
                        result = {}
                        result['data'] = __message_out
                        result['min_proc_time_ms'] = 0
                        result['max_proc_time_ms'] = 0

                elif __endpoint == 'kline':
                    __message_out = self.manage_websocket_message_kline(__temp_data)

                    if __message_out is not None:
                        result = {}
                        result['data'] = __message_out[:self.__result_max_len]
                        result['min_proc_time_ms'] = 0
                        result['max_proc_time_ms'] = 0

                elif __endpoint == 'trades':
                    __message_out = self.manage_websocket_message_trades(__temp_data)

                    if __message_out is not None:
                        result = {}
                        result['data'] = __message_out[:self.__result_max_len]
                        result['min_proc_time_ms'] = 0
                        result['max_proc_time_ms'] = 0

                elif __endpoint == 'ticker':
                    __message_out = self.manage_websocket_message_ticker(__temp_data)

                    if __message_out is not None:
                        result = {}
                        result['data'] = __message_out
                        result['min_proc_time_ms'] = 0
                        result['max_proc_time_ms'] = 0

        except Exception as exc: # pylint: disable=broad-except
            print(str(exc))

        return result

    def __manage_websocket_diff_data(self, diff_data):
        """
        __manage_websocket_diff_data
        ============================
            This function manage websocket message and normalize result data.

                :param diff_data: dict websocket message data.
                
                :return bool: Return True if no error ocurred.
        """

        result = False

        if isinstance(diff_data,dict) and 'u' in diff_data and 'b' in diff_data\
            and 'a' in diff_data and 's' in diff_data:

            __stream_index = self.get_stream_index('order_book', diff_data['s'])

            if isinstance(diff_data['b'],list) and isinstance(diff_data['a'],list)\
                and __stream_index in self.__ws_temp_data\
                and self.__ws_temp_data[__stream_index] is not None\
                and isinstance(self.__ws_temp_data[__stream_index], dict):
                current_data = self.__ws_temp_data[__stream_index]
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
                self.__ws_temp_data[__stream_index] = current_data
                result = True

        return result
