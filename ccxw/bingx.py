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
import threading
import websocket_server

import ccxw.ccxw_common_functions as ccf

class BingxAuxClass():
    """
    Ccxw - CryptoCurrency eXchange Websocket Library BingxAuxClass
    ==============================================================
        This class contains helper functions for the Ccxw class.
    """

    def __init__(self, endpoint: str=None, symbol: str=None, trading_type: str='SPOT',\
                 testmode: bool=False, api_key: str=None, api_secret: str=None,\
                    result_max_len: int=5, update_speed: str='100ms', interval: str='1m',
                    data_max_len: int=1000, debug: bool=False):
        """
        BingxAuxClass constructor
        =========================
            Initializes the BingxAuxClass with the provided parameters.

                :param self: BingxAuxClass instance.
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

                :return: Return a new instance of the Class BingxAuxClass.
        """


        self.__exchange = os.path.basename(__file__)[:-3]
        self.__ws_endpoint = endpoint
        self.__ws_server = None
        self.__ws_server_url = '127.0.0.1'
        self.__ws_server_port = 49001
        self.__stop_launcher = False
        self.__thread = None

        ##self.__api_base_url = 'https://open-api.bingx.com'

        self.__api_key = api_key # pylint: disable=unused-private-member
        self.__api_secret = api_secret # pylint: disable=unused-private-member
        self.__testmode = testmode

        self.__debug = debug # pylint: disable=unused-private-member
        self.__ws_symbol = symbol
        self.__trading_type = trading_type
        self.__data_max_len = data_max_len

        if self.__data_max_len > 400:
            self.__data_max_len = 400

        if self.__data_max_len < 1:
            self.__data_max_len = 1

        self.__result_max_len = result_max_len
        self.__update_speed = update_speed
        self.__interval = interval

        if self.__result_max_len > 500:
            self.__result_max_len = 500

        if self.__result_max_len < 1:
            self.__result_max_len = 1

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
        self.__stop_launcher = True
        if self.__thread is not None and threading.current_thread() is not self.__thread\
            and self.__thread.is_alive():
            try:
                self.__thread.join(9)
            except Exception: # pylint: disable=broad-except
                pass

    def stop(self):
        """
        Stopping websocket server 
        =========================

            :param self: BingxAuxClass instance.
        """

        self.__stop_launcher = True

    def get_websocket_url(self):
        """
        get_websocket_url
        =================
            This function set and return websocket URL.

                :return str: Return websocket URL.
        """

        result = None

        __local_ws_url = 'ws://' + self.__ws_server_url + ':' + str(self.__ws_server_port)

        if self.__trading_type == 'SPOT':
            if self.__ws_endpoint == 'trades':
                self.__ws_url_api = __local_ws_url
                self.__ws_url_test = ''
            elif self.__ws_endpoint == 'ticker':
                self.__ws_url_api = __local_ws_url
                self.__ws_url_test = ''
            else:
                self.__ws_url_api = 'wss://open-api-ws.bingx.com/market'
                self.__ws_url_test = ''

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
        __l_url_api = self.get_api_url()

        __l_endpoint = '/openApi/' + self.__trading_type.lower() + '/v1/common/symbols'

        __l_url_point = __l_url_api + __l_endpoint
        if self.__ws_symbol is not None and isinstance(self.__ws_symbol,str):
            __l_url_point = __l_url_point + '?symbol='\
                + str(self.__ws_symbol.replace('/','-').upper())

        __data = ccf.file_get_contents_url(__l_url_point)

        if __data is not None and ccf.is_json(__data):
            result = json.loads(__data)

        return result

    def if_symbol_supported(self):
        """
        if_symbol_supported
        ===================
            This function check if symbol is supported by the exchange.

                :return bool: Return True if supported 
        """

        result = False

        __data = self.get_exchange_info()

        if isinstance(__data,dict) and 'code' in __data and __data['code'] == 0\
            and 'data' in __data and isinstance(__data['data'], dict)\
            and 'symbols' in __data['data'] and isinstance(__data['data']['symbols'],list)\
            and len(__data['data']['symbols']) > 0\
            and isinstance(__data['data']['symbols'][0],dict):
            if 'symbol' in __data['data']['symbols'][0]\
                and isinstance(__data['data']['symbols'][0]['symbol'],str)\
                and self.__ws_symbol.replace('/','-').upper() ==\
                    __data['data']['symbols'][0]['symbol']:
                if 'status' in __data['data']['symbols'][0]\
                    and isinstance(__data['data']['symbols'][0]['status'],int)\
                    and __data['data']['symbols'][0]['status'] == 1:
                    result = True

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

        if not (self.__update_speed == '100ms' or self.__update_speed == '1000ms'):
            self.__update_speed = '1000ms'

        if self.__ws_endpoint == 'order_book':
            __send_data_vars = None
            __send_data_vars = {}
            __send_data_vars['id'] = str(time.time_ns())
            __send_data_vars['reqType'] = 'sub'
            __send_data_vars['dataType'] = self.__ws_symbol.replace("/","-").upper()\
                + '@depth' + '100'

            self.__ws_endpoint_url = ''
            self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)

        elif self.__ws_endpoint == 'kline':

            ##__out_interval = self.__interval
            __out_interval = '1min' # Only available for 1 minute

            __send_data_vars = None
            __send_data_vars = {}
            __send_data_vars['id'] = str(time.time_ns())
            __send_data_vars['reqType'] = 'sub'
            __send_data_vars['dataType'] = self.__ws_symbol.replace("/","-").upper()\
                + '@kline_' + str(__out_interval)

            self.__ws_endpoint_url = ''
            self.__ws_endpoint_on_open_vars = json.dumps(__send_data_vars)

        elif self.__ws_endpoint == 'trades':
            __send_data_vars = None

            self.__ws_endpoint_url = '/'
            self.__ws_endpoint_on_open_vars = None

            self.__ws_server = websocket_server.WebsocketServer(port=self.__ws_server_port)

            self.__ws_server.run_forever(threaded=True)

            self.__stop_launcher = False
            self.__thread = threading.Thread(target=self.__get_local_websocket_trades_data)
            self.__thread.start()

        elif self.__ws_endpoint == 'ticker':
            __send_data_vars = None

            self.__ws_endpoint_url = '/'
            self.__ws_endpoint_on_open_vars = None

            self.__ws_server = websocket_server.WebsocketServer(port=self.__ws_server_port)

            self.__ws_server.run_forever(threaded=True)

            self.__stop_launcher = False
            self.__thread = threading.Thread(target=self.__get_local_websocket_ticker_data)
            self.__thread.start()

        if __send_data_vars is not None and isinstance(__send_data_vars,dict):
            self.__ws_endpoint_on_close_vars = __send_data_vars
            self.__ws_endpoint_on_close_vars['reqType'] = 'unsub'
            self.__ws_endpoint_on_close_vars = json.dumps(self.__ws_endpoint_on_close_vars)

        result = {}
        result['ws_endpoint_url'] = self.__ws_endpoint_url
        result['ws_endpoint_on_open_vars'] = self.__ws_endpoint_on_open_vars
        result['ws_endpoint_on_close_vars'] = self.__ws_endpoint_on_close_vars

        return result

    def __get_local_websocket_trades_data(self):
        """
        __get_local_websocket_trades_data
        =================================
            This function get trades data from Rest API and send data to a local websocket server
            Requests limit 500 per minute and 1500 per 5 minutes -> 5 per second
            In our case 2 requests per second max.

                :return bool: True if all OK
        """

        result = True
        __min_time = 0.5
        __timeout = 4

        __timestamp = str(round(time.time_ns() / 1000000))

        __l_endpoint = '/openApi/' + str(self.__trading_type).lower() + '/v1/market/trades?symbol='\
            + self.__ws_symbol.replace("/","-").upper() + '&limit=100'
        __data_url = self.__url_api + __l_endpoint

        if self.__testmode:
            __data_url = self.__url_test + __l_endpoint

        while not self.__stop_launcher:
            __time_ini = time.time_ns()
            __message = ccf.file_get_contents_url(__data_url, 'b', None, {}, __timeout)
            ##__message = str(__message.decode('utf-8'))

            if __message is not None and ccf.is_json(__message):
                self.__ws_server.send_message_to_all(__message)

            __time_end = time.time_ns()
            __time_diff = (__time_end - __time_ini) / 1000000000
            __time_sleep = round((__min_time - __time_diff),3)

            if __time_sleep > 0:
                time.sleep(__time_sleep)

        if self.__stop_launcher:
            self.__ws_server.shutdown_gracefully()
            #self.__ws_server.shutdown_abruptly()

        return result

    def __get_local_websocket_ticker_data(self):
        """
        __get_local_websocket_ticker_data
        =================================
            This function get ticker data from Rest API and send data to a local websocket server
            Requests limit 500 per minute and 1500 per 5 minutes -> 5 per second
            In our case 2 requests per second max.

                :return bool: True if all OK
        """

        result = True
        __min_time = 0.5
        __timeout = 4

        __timestamp = str(round(time.time_ns() / 1000000))

        __l_endpoint = '/openApi/' + str(self.__trading_type).lower()\
            + '/v1/ticker/24hr?timestamp='\
            + __timestamp + '&symbol=' + self.__ws_symbol.replace("/","-").upper()
        __data_url = self.__url_api + __l_endpoint
        if self.__testmode:
            __data_url = self.__url_test + __l_endpoint

        while not self.__stop_launcher:
            __time_ini = time.time_ns()
            __message = ccf.file_get_contents_url(__data_url, 'b', None, {}, __timeout)
            ##__message = str(__message.decode('utf-8'))

            if __message is not None and ccf.is_json(__message):
                self.__ws_server.send_message_to_all(__message)

            __time_end = time.time_ns()
            __time_diff = (__time_end - __time_ini) / 1000000000
            __time_sleep = round((__min_time - __time_diff),3)

            if __time_sleep > 0:
                time.sleep(__time_sleep)

        if self.__stop_launcher:
            self.__ws_server.shutdown_gracefully()
            #self.__ws_server.shutdown_abruptly()

        return result

    def manage_websocket_message(self,ws,message_in):
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

                    if self.__ws_endpoint == 'order_book':
                        self.__ws_temp_data = __temp_data
                        if __temp_data is not None and isinstance(__temp_data,dict)\
                            and 'dataType' in __temp_data and 'data' in __temp_data\
                            and isinstance(__temp_data['data'],dict):
                            __bids = __temp_data['data']['bids']
                            __asks = __temp_data['data']['asks']
                            __asks.reverse()

                            __message_out = None
                            __message_out = {}
                            __message_out['endpoint'] = self.__ws_endpoint
                            __message_out['exchange'] = self.__exchange
                            __message_out['symbol'] = self.__ws_symbol
                            __message_out['interval'] = None
                            __message_out['last_update_id'] = str(time.time_ns())
                            __message_out['diff_update_id'] = 0
                            __message_out['bids'] = __bids[:self.__result_max_len]
                            __message_out['asks'] = __asks[:self.__result_max_len]
                            __message_out['type'] = 'snapshot'
                            __current_datetime = datetime.datetime.utcnow()
                            __current_timestamp = __current_datetime.strftime("%s.%f")
                            __current_datetime = __current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
                            __message_out['timestamp'] = __current_timestamp
                            __message_out['datetime'] = __current_datetime

                            self.__ws_temp_data = __temp_data

                            result = {}
                            result['data'] = __message_out
                            result['min_proc_time_ms'] = 0
                            result['max_proc_time_ms'] = 0

                    elif self.__ws_endpoint == 'kline':
                        if self.__ws_temp_data is None or not isinstance(self.__ws_temp_data,dict):
                            self.__ws_temp_data = {}

                        ##pprint.pprint(__temp_data,sort_dicts=False)
                        if __temp_data is not None and isinstance(__temp_data,dict)\
                            and 'code' in __temp_data and int(__temp_data['code']) == 0\
                            and 'dataType' in __temp_data and 'data' in __temp_data\
                            and isinstance(__temp_data['data'],dict) and 'E' in __temp_data['data']\
                            and 'K' in __temp_data['data']\
                            and isinstance(__temp_data['data']['K'],dict):

                            ##pprint.pprint(__temp_data,sort_dicts=False)

                            __out_interval = '1m' # Only available for 1 minute

                            __message_add = None
                            __message_add = {}
                            __message_add['endpoint'] = self.__ws_endpoint
                            __message_add['exchange'] = self.__exchange
                            __message_add['symbol'] = self.__ws_symbol
                            __message_add['interval'] = __out_interval
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

                            self.__ws_temp_data[int(__message_add['open_time'])] = __message_add

                            while len(self.__ws_temp_data) > self.__data_max_len:
                                __first_key = min(list(self.__ws_temp_data.keys()))
                                __nc = self.__ws_temp_data.pop(__first_key,None)

                            __message_out = list(self.__ws_temp_data.values())

                            result = {}
                            result['data'] = __message_out
                            result['min_proc_time_ms'] = 0
                            result['max_proc_time_ms'] = 0

                    elif self.__ws_endpoint == 'trades':

                        if __temp_data is not None and isinstance(__temp_data,dict)\
                            and 'code' in __temp_data and  int(__temp_data['code']) == 0\
                            and 'timestamp' in __temp_data and 'data' in __temp_data\
                            and isinstance(__temp_data['data'],list)\
                            and len(__temp_data['data']) > 0:

                            if self.__ws_temp_data is None:
                                self.__ws_temp_data = {}

                            for i in range(len(__temp_data['data']) - 1,-1,-1):
                                __message_add = None
                                __message_add = {}
                                __message_add['endpoint'] = self.__ws_endpoint
                                __message_add['exchange'] = self.__exchange
                                __message_add['symbol'] = self.__ws_symbol
                                __message_add['interval'] = None
                                __message_add['event_time'] = __temp_data['timestamp']
                                __message_add['trade_id'] = __temp_data['data'][i]['id']
                                __message_add['price'] = __temp_data['data'][i]['price']
                                __message_add['quantity'] = __temp_data['data'][i]['qty']
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

                                self.__ws_temp_data[int(__message_add['trade_id'])] = __message_add

                                while len(self.__ws_temp_data) > self.__data_max_len:
                                    __first_key = min(list(self.__ws_temp_data.keys()))
                                    __nc = self.__ws_temp_data.pop(__first_key,None)

                                __message_out = list(self.__ws_temp_data.values())
                                __message_out.reverse()

                            result = {}
                            result['data'] = __message_out
                            result['min_proc_time_ms'] = 0
                            result['max_proc_time_ms'] = 0

                    elif self.__ws_endpoint == 'ticker':

                        if __temp_data is not None and isinstance(__temp_data,dict)\
                            and 'code' in __temp_data and  int(__temp_data['code']) == 0\
                            and 'timestamp' in __temp_data and 'data' in __temp_data\
                            and isinstance(__temp_data['data'],list)\
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
                            __message_add['last_price'] = __temp_data['data'][i]['lastPrice']
                            __message_add['last_quantity'] = None
                            __message_add['best_bid_price'] = None
                            __message_add['best_bid_quantity'] = None
                            __message_add['best_ask_price'] = None
                            __message_add['best_ask_quantity'] = None
                            __message_add['open_price'] = __temp_data['data'][i]['openPrice']
                            __message_add['high_price'] = __temp_data['data'][i]['highPrice']
                            __message_add['low_price'] = __temp_data['data'][i]['lowPrice']
                            __message_add['total_traded_base_asset_volume'] = (
                                __temp_data['data'][i]['volume']
                            )
                            __message_add['total_traded_quote_asset_volume'] = (
                                __temp_data['data'][i]['quoteVolume']
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

                            result = {}
                            result['data'] = __message_out
                            result['min_proc_time_ms'] = 0
                            result['max_proc_time_ms'] = 0

        except Exception as exc: # pylint: disable=broad-except
            print(str(exc))

        return result
