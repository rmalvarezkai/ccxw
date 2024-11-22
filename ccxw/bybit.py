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
import threading
import ccxw.ccxw_common_functions as ccf
from ccxw.safe_thread_vars import DictSafeThread
import ccxw

class BybitCcxwAuxClass():
    """
    Ccxw - CryptoCurrency eXchange Websocket Library BybitCcxwAuxClass
    ==================================================================
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
        BybitCcxwAuxClass constructor
        =============================
            :param self: BybitCcxwAuxClass instance.
            :param streams: list[dict]
                                    dicts must have this struct.
                                        {
                                            'endpoint': str only allowed 'order_book' | 'kline' |\
                                                'trades' | 'ticker',
                                            'symbol': str unified symbol.,
                                            'interval': str '1m' | '3m' | '5m' | '15m' | '30m' |\
                                                '1h' | '2h' | '4h' | '6h' | '12h' | '1d' | '1w' |\
                                                '1mo' for 'kline' endpoint is mandatory.
                                        }

            :param trading_type: str only allowed 'SPOT'.
            :param testmode: bool.
            :param result_max_len: int Max return values > 1 and <= data_max_len.
            :param data_max_len: int. > 1 and <= 2500 max len of data getting from exchange.
            :param debug: bool Verbose output.

            :return: Return a new instance of the Class BybitCcxwAuxClass.
        """
        __exchange_limit_streams = 10

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

        self.__api_type = 'public'

        self.__ws_url_api = None
        self.__ws_url_test = None
        self.__ws_url = None # pylint: disable=unused-private-member
        self.__url_api = None
        self.__url_test = None
        self.__ws_endpoint_url = None
        self.__ws_endpoint_on_open_vars = None
        self.__ws_endpoint_on_close_vars = None
        self.__ws_endpoint_on_auth_vars = None
        self.__ws_temp_data = DictSafeThread()

        self.__stop_flag = False
        self.__stop_flag_lock = threading.Lock()

        self.__ping_thread = None

        self.__ws = None
        self.__ws_lock = threading.Lock()

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

        if not self.__stop_flag:
            self.stop()

    def start(self):
        """
        start
        =====
        """

        with self.__stop_flag_lock:
            self.__stop_flag = False

        self.__ping_thread = threading.Thread(target=self.__thread_ping,\
                                              name='ccxw_bybit_ping_thread')

        self.__ping_thread.start()


    def stop(self):
        """
        stop
        ====
        """

        with self.__stop_flag_lock:
            self.__stop_flag = True

        if self.__ping_thread is not None:
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
                                            '1h' | '2h' | '4h' | '6h' | '12h' | '1d' | '1w' | '1mo'\
                                            for 'kline' endpoint is mandatory.
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
                             '6h', '12h', '1d', '1w', '1mo']

        return result

    def get_websocket_url(self):
        """
        get_websocket_url
        =================
            This function set and return websocket URL.

                :return str: Return websocket URL.
        """

        result = None

        self.__ws_url_api = 'wss://stream.bybit.com/v5/'\
            + str(self.__api_type) + '/'\
            + self.__trading_type.lower()
        self.__ws_url_test = 'wss://stream-testnet.bybit.com/v5/'\
            + str(self.__api_type) + '/'\
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

            __l_endpoint = '/v5/market/instruments-info?category=' + self.__trading_type.lower()

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

    def __get_interval_from_unified_interval(self, interval):
        result = '1'
        result = str(interval)
        if 'm' in result:
            result = result.replace('m', '')
        elif 'h' in result or 'H' in result:
            result = result.replace('h', '').replace('H','')
            result = 60 * int(result)
        elif 'd' in result or 'D' in result:
            result = 'D'
        elif 'w' in result or 'W' in result:
            result = 'W'
        elif 'M' in result:
            result = 'M'

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

        result = str(interval)

        if result is not None:
            if result.isnumeric():
                result = int(result)

                if result == 1:
                    result = '1m'
                elif result < 60:
                    result = str(result) + 'm'
                else:
                    result = str(round(result/60)) + 'h'

            elif 'M' in result:
                result = '1mo'

            elif 'w' in result or 'W' in result:
                result = '1w'

            elif 'd' in result or 'D' in result:
                if result.lower() == 'd':
                    result = '1d'
                else:
                    result = result.lower()

            result = str(result)

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
        __send_data_vars['op'] = 'subscribe'
        __send_data_vars['args'] = []
        __send_data_vars['req_id'] = str(time.time_ns())

        self.__ws_endpoint_url = ''

        for stream in self.__ws_streams:
            interval = 'none'

            if stream['endpoint'] == 'order_book':
                result = True

                __send_data_vars['args'].append('orderbook.' + str(50) + '.'\
                                                + stream['symbol'].replace("/","").upper())

            elif stream['endpoint'] == 'kline':
                result = True
                interval = stream['interval']
                interval_out = self.__get_interval_from_unified_interval(interval)

                __send_data_vars['args'].append('kline.' + str(interval_out)\
                                                + '.' + stream['symbol'].replace("/","").upper())

                self.__ws_endpoint_url = ''
                self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)
            elif stream['endpoint'] == 'trades':
                result = True

                __send_data_vars['args'].append('publicTrade' + '.'\
                                                + stream['symbol'].replace("/","").upper())

                self.__ws_endpoint_url = ''
                self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)

            elif stream['endpoint'] == 'ticker':
                result = True

                __send_data_vars['args'].append('tickers' + '.'\
                                                + stream['symbol'].replace("/","").upper())

                self.__ws_endpoint_url = ''
                self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)

            __stream_index = self.get_stream_index(stream['endpoint'],\
                                                     stream['symbol'],\
                                                     interval=interval)
            self.__ws_temp_data[__stream_index] = None

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

    def __send_ping(self):
        result = False

        __local_stop = True

        with self.__stop_flag_lock:
            __local_stop = self.__stop_flag

        __id = ccf.random_string(9) + str(round(time.time() * 1000000))
        __req_ping = {
            'req_id': __id,
            'op': 'ping'
        }

        __req_ping = json.dumps(__req_ping, sort_keys=False)

        try:
            with self.__ws_lock:
                if self.__ws is not None and not __local_stop:
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
        __ping_interval = 20
        __local_ws = None

        while not __local_stop:

            if self.__send_ping():
                __sleep_time = __ping_interval - abs(time.time() - __last_ping_time)
                __last_ping_time = time.time()
            else:
                __sleep_time = 1

            __sleep_time -= 1

            if __sleep_time <= 0:
                __sleep_time = 1

            time.sleep(__sleep_time)

            with self.__ws_lock:
                __local_ws = self.__ws

            with self.__stop_flag_lock:
                __local_stop = self.__stop_flag

    def __init_order_book_data(self, temp_data):

        result = False

        __symbol = temp_data['topic'].split('.')[2]
        __stream_index = self.get_stream_index('order_book', __symbol)

        self.__ws_temp_data[__stream_index] = temp_data
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

        __symbol = ''
        __stream_index = None

        if __temp_data is not None and isinstance(__temp_data,dict)\
            and 'type' in __temp_data and len(__temp_data['topic'].split('.')) >= 3:
            __symbol = __temp_data['topic'].split('.')[2]
            __stream_index = self.get_stream_index('order_book', __symbol)

            if self.__ws_temp_data[__stream_index] is None or __temp_data['type'] == 'snapshot':
                if self.__init_order_book_data(__temp_data):
                    __proc_data = True
            elif self.__ws_temp_data[__stream_index] is not None\
                and self.__manage_websocket_diff_data(__temp_data):
                __diff_update_id = (
                    __temp_data['data']['u'] - self.__ws_temp_data[__stream_index]['data']['u']
                )
                __data_type = 'update'
                __proc_data = True

        if __proc_data:
            __message_out = None
            __message_out = {}
            __message_out['endpoint'] = 'order_book'
            __message_out['exchange'] = self.__exchange
            __message_out['symbol'] = __symbol
            __message_out['interval'] = None
            __message_out['last_update_id'] = self.__ws_temp_data[__stream_index]['data']['u']
            __message_out['diff_update_id'] = __diff_update_id
            __message_out['bids'] = (
                self.__ws_temp_data[__stream_index]['data']['b'][:self.__result_max_len]
            )
            __message_out['asks'] = (
                self.__ws_temp_data[__stream_index]['data']['a'][:self.__result_max_len]
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

        __symbol = ''
        __stream_index = None

        if __temp_data is not None and isinstance(__temp_data,dict):
            if 'type' in __temp_data and 'data' in __temp_data\
                and isinstance(__temp_data['data'],list) and len(__temp_data['data']) > 0\
                and len(__temp_data['topic'].split('.')) >= 3:

                __symbol = __temp_data['topic'].split('.')[2]
                __interval = __temp_data['topic'].split('.')[1]
                __stream_index = self.get_stream_index('kline', __symbol, __interval)

                if self.__ws_temp_data[__stream_index] is None\
                    or not isinstance(self.__ws_temp_data[__stream_index],dict):

                    self.__ws_temp_data[__stream_index] = {}

                for i in range(0,len(__temp_data['data'])):
                    __message_add = None
                    __message_add = {}
                    __message_add['endpoint'] = 'kline'
                    __message_add['exchange'] = self.__exchange
                    __message_add['symbol'] = __symbol
                    __message_add['interval'] = __interval
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

                    self.__ws_temp_data[__stream_index][int(__message_add['open_time'])] = (
                        __message_add
                    )

                while len(self.__ws_temp_data[__stream_index]) > self.__data_max_len:
                    __first_key = min(list(self.__ws_temp_data[__stream_index].keys()))
                    __nc = self.__ws_temp_data[__stream_index].pop(__first_key,None)

                __message_out = (
                    list(self.__ws_temp_data[__stream_index].values())[:self.__result_max_len]
                )

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

        __symbol = ''
        __stream_index = None

        if __temp_data is not None and isinstance(__temp_data,dict):
            if 'ts' in __temp_data and 'data' in __temp_data\
                and isinstance(__temp_data['data'],list) and len(__temp_data['data']) > 0\
                and len(__temp_data['topic'].split('.')) >= 2:

                __symbol = __temp_data['topic'].split('.')[1]
                __stream_index = self.get_stream_index('trades', __symbol)

                if self.__ws_temp_data[__stream_index] is None:
                    self.__ws_temp_data[__stream_index] = queue.Queue(maxsize=self.__data_max_len)

                for i in range(len(__temp_data['data']) - 1,-1,-1):
                    __message_add = None
                    __message_add = {}
                    __message_add['endpoint'] = 'trades'
                    __message_add['exchange'] = self.__exchange
                    __message_add['symbol'] = __symbol
                    __message_add['interval'] = None
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

        __symbol = ''
        __stream_index = None

        if __temp_data is not None and isinstance(__temp_data,dict):
            if 'cs' in __temp_data and 'data' in __temp_data\
                and isinstance(__temp_data['data'],dict) and len(__temp_data['data']) > 0\
                and len(__temp_data['topic'].split('.')) >= 2:

                __symbol = __temp_data['topic'].split('.')[1]
                __stream_index = self.get_stream_index('ticker', __symbol)

                self.__ws_temp_data[__stream_index] = __temp_data

                __message_add = None
                __message_add = {}
                __message_add['endpoint'] = 'ticker'
                __message_add['exchange'] = self.__exchange
                __message_add['symbol'] = __symbol
                __message_add['interval'] = None
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

                __endpoint = 'NONE'
                if isinstance(__temp_data, dict):
                    if 'topic' in __temp_data\
                        and __temp_data['topic'] is not None\
                        and isinstance(__temp_data['topic'], str):

                        __tmp_endpoint = __temp_data['topic'].split('.')[0]

                        if __tmp_endpoint == 'orderbook':
                            __endpoint = 'order_book'
                        elif __tmp_endpoint == 'kline':
                            __endpoint = 'kline'
                        elif __tmp_endpoint == 'publicTrade':
                            __endpoint = 'trades'
                        elif __tmp_endpoint == 'tickers':
                            __endpoint = 'ticker'
                    elif 'ret_msg' in __temp_data\
                        and 'op' in __temp_data\
                        and __temp_data['ret_msg'] is not None\
                        and __temp_data['op'] is not None\
                        and __temp_data['ret_msg'] == 'pong'\
                        and __temp_data['op'] == 'ping':
                        __endpoint = 'ping'

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
        """
        __manage_websocket_diff_data
        ============================
            This function manage websocket message and normalize result data.

                :param diff_data: dict websocket message data.

                :return bool: Return True if no error ocurred.
        """

        result = False
        __symbol = diff_data['topic'].split('.')[2]
        __stream_index = self.get_stream_index('order_book', __symbol)

        current_data = self.__ws_temp_data[__stream_index]

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
                self.__ws_temp_data[__stream_index] = current_data
                result = True

        return result
