# pylint: disable=duplicate-code
"""
CCXW - CryptoCurrency eXchange Websocket Library
kline tests cases.

Author: Ricardo Marcelo Alvarez
Date: 2023-10-31
poetry run python -m unittest tests/test_ccxw.py
"""
import sys
import unittest
import time
import pprint

from schema import Schema, And, Or
from ccxw import Ccxw

class TestCcxw(unittest.TestCase):
    """
    TestCcxw - Auxiliary class for testing Ccxw
    ===========================================
        This class contains helper functions for testing the Ccxw class.
    """

    @classmethod
    def setUpClass(cls):

        cls.__debug = False

        cls.__wsm = {}

        __symbols = ['BTC/USDT']
        __testmode = False
        __intervals = ['1m', '1h', '1d']
        __result_max_len = 500
        __update_speed = '1000ms'
        __data_max_len = 500
        __debug = False

        cls.__exchanges = Ccxw.get_supported_exchanges()
        cls.__endpoints = Ccxw.get_supported_endpoints()

        cls.__streams = []

        for symbol in __symbols:
            for endpoint in cls.__endpoints:
                if endpoint == 'kline':
                    for __interval in __intervals:
                        stream_add = None
                        stream_add = {
                                'endpoint': endpoint,
                                'symbol': symbol,
                                'interval': __interval
                        }
                        cls.__streams.append(stream_add)
                else:
                    stream_add = None
                    stream_add = {
                            'endpoint': endpoint,
                            'symbol': symbol
                    }
                    cls.__streams.append(stream_add)

        for exchange in cls.__exchanges:

            cls.__wsm[exchange] = Ccxw(exchange, cls.__streams, testmode=__testmode,\
                                       result_max_len=__result_max_len,\
                                       data_max_len=__data_max_len,\
                                       debug=__debug)

            cls.__wsm[exchange].start()

        time.sleep(9) # For collect some data

    @classmethod
    def tearDownClass(cls):
        for exchange in cls.__exchanges:
            cls.__wsm[exchange].stop()

    def __is_numeric_string(self, value): # pylint: disable=unused-private-member
        result = False
        try:
            float_value = float(value) # pylint: disable=unused-variable
            result = True
        except ValueError:
            result = False

        return result

    def __check_bids_and_asks(self, data, type_c='bids'):
        result = False

        numeric_data = []

        for duple in data:
            if isinstance(duple,list) and len(duple) >= 2:
                if self.__is_numeric_string(duple[0]) and self.__is_numeric_string(duple[1]):
                    numeric_data.append(float(duple[0]))
                    result = True

        if result:
            if len(numeric_data) >= 2:
                if type_c == 'bids':
                    result = all(x > y for x, y in zip(numeric_data, numeric_data[1:]))
                elif type_c == 'asks':
                    result = all(x < y for x, y in zip(numeric_data, numeric_data[1:]))

        return result

    def __check_data_structure_order_book(self, data):

        result = False

        schema = Schema({'data': Or(And(dict, len),None),
                         'min_proc_time_ms': (float),
                         'max_proc_time_ms': (float)
                        }
        )

        schema_data = Schema({'endpoint': str,
                              'exchange': str,
                              'symbol': str,
                              'interval': Or(str,None),
                              'last_update_id': int,
                              'diff_update_id': int,
                              'bids': list,
                              'asks': list,
                              'type': str,
                              'timestamp': str,
                              'datetime': str
                              }
                            )

        if data is not None and schema.is_valid(data)\
            and data['data'] is not None\
            and schema_data.is_valid(data['data']):
            if self.__check_bids_and_asks(data['data']['bids'], 'bids')\
                and self.__check_bids_and_asks(data['data']['asks'], 'asks')\
                and self.__is_numeric_string(data['data']['timestamp']):

                result = True

        return result

    def __check_data_structure_kline(self, data):
        result = False

        schema = Schema({'data': Or(And(list, len),None),
                         'min_proc_time_ms': (float),
                         'max_proc_time_ms': (float)
                        }
        )

        schema_data = Schema({'endpoint': str,
                              'exchange': str,
                              'symbol': str,
                              'interval': Or(str,None),
                              'last_update_id': Or(int,None),
                              'open_time': Or(int,None),
                              'close_time': Or(int,None),
                              'open_time_date': Or(str,None),
                              'close_time_date': Or(str,None),
                              'open': Or(str,None),
                              'close': Or(str,None),
                              'hight': Or(str,None),
                              'low': Or(str,None),
                              'volume': Or(str,None),
                              'is_closed': Or(bool,None)
                            }
                        )

        numeric_data = []

        if data is not None and schema.is_valid(data):
            if data['data'] is not None and len(data['data']) > 0:
                result = True
                for node_data in data['data']:
                    result = result and schema_data.is_valid(node_data)

                    result = (
                        result and (node_data['open'] is None\
                                    or self.__is_numeric_string(node_data['open']))
                    )
                    result = (
                        result and (node_data['close'] is None\
                                    or self.__is_numeric_string(node_data['close']))
                    )

                    result = (
                        result and (node_data['hight'] is None\
                                    or self.__is_numeric_string(node_data['hight']))
                        )

                    result = (
                        result and (node_data['low'] is None\
                                    or self.__is_numeric_string(node_data['low']))
                    )

                    result = (
                        result and (node_data['volume'] is None\
                                    or self.__is_numeric_string(node_data['volume']))
                    )

                    numeric_data.append(float(node_data['open_time']))

            else:
                result = True

        if result:
            if len(numeric_data) >= 2:
                result = all(x < y for x, y in zip(numeric_data, numeric_data[1:]))

        return result

    def __check_data_structure_trades(self, data):
        result = False

        schema = Schema({'data': Or(And(list, len),None),
                         'min_proc_time_ms': (float),
                         'max_proc_time_ms': (float)
                        }
        )

        schema_data = Schema({'endpoint': str,
                              'exchange': str,
                              'symbol': str,
                              'interval': Or(str,None),
                              'event_time': Or(int,None),
                              'trade_id': Or(str,None),
                              'price': Or(str,None),
                              'quantity': Or(str,None),
                              'trade_time': Or(int,None),
                              'trade_time_date': Or(str,None),
                              'side_of_taker': Or(str,None)
                            }
                        )

        numeric_data = []

        if data is not None and schema.is_valid(data):
            if data['data'] is not None and len(data['data']) > 0:
                result = True
                for node_data in data['data']:
                    result = result and schema_data.is_valid(node_data)
                    result = (
                        result and (node_data['price'] is None\
                                    or self.__is_numeric_string(node_data['price']))
                    )
                    result = (
                        result and (node_data['quantity'] is None\
                                    or self.__is_numeric_string(node_data['quantity']))
                    )

                    numeric_data.append(float(node_data['trade_time']))

            else:
                result = True

        if result:
            if len(numeric_data) >= 2:
                result = all(x <= y for x, y in zip(numeric_data, numeric_data[1:]))

        return result

    def __check_data_structure_ticker(self, data):

        result = False

        schema = Schema({'data': Or(And(dict, len), None),
                         'min_proc_time_ms': float,
                         'max_proc_time_ms': float
                        }
        )

        schema_data = Schema({'endpoint': str,
                              'exchange': str,
                              'symbol': str,
                              'interval': Or(str, None),
                              'event_type': Or(str, None),
                              'event_time': Or(int, None),
                              'event_time_date': Or(str, None),
                              'price_change': Or(str, None),
                              'price_change_percent': Or(str, None),
                              'weighted_average_price': Or(str, None),
                              'first_trade_before_the_24hr_rolling_window': Or(str, None),
                              'last_price': Or(str, None),
                              'last_quantity': Or(str, None),
                              'best_bid_price': Or(str, None),
                              'best_bid_quantity': Or(str, None),
                              'best_ask_price': Or(str, None),
                              'best_ask_quantity': Or(str, None),
                              'open_price': Or(str, None),
                              'high_price': Or(str, None),
                              'low_price': Or(str, None),
                              'total_traded_base_asset_volume': Or(str, None),
                              'total_traded_quote_asset_volume': Or(str, None),
                              'statistics_open_time': Or(int, None),
                              'statistics_open_time_date': Or(str, None),
                              'statistics_close_time': Or(int, None),
                              'statistics_close_time_date': Or(str, None),
                              'total_number_of_trades': Or(int, None)
                            }
                        )

        if data is not None and schema.is_valid(data)\
             and data['data'] is not None and schema_data.is_valid(data['data']):

            result = all(
                data['data'][key] is None or self.__is_numeric_string(data['data'][key])
                for key in [
                    'price_change',
                    'price_change_percent',
                    'weighted_average_price',
                    'first_trade_before_the_24hr_rolling_window',
                    'last_price',
                    'last_quantity',
                    'best_bid_price',
                    'best_bid_quantity',
                    'best_ask_price',
                    'best_ask_quantity',
                    'open_price',
                    'high_price',
                    'low_price',
                    'total_traded_base_asset_volume',
                    'total_traded_quote_asset_volume',
                ]
            )

        return result

    def __check_data_structure_local(self, endpoint, data):
        result = False

        if endpoint == 'order_book':
            result = self.__check_data_structure_order_book(data)
        elif endpoint == 'kline':
            result = self.__check_data_structure_kline(data)
        elif endpoint == 'trades':
            result = self.__check_data_structure_trades(data)
        elif endpoint == 'ticker':
            result = self.__check_data_structure_ticker(data)

        return result

    def test_binance(self):
        """
        test_binance
        ============
            Test case for binance exchange.
        """

        __exchg = sys._getframe().f_code.co_name[5:] # pylint: disable=protected-access

        __data_struct_res = False
        error_msg = ''

        for stream in self.__streams:
            if stream is not None\
                and isinstance(stream, dict)\
                and 'endpoint' in stream\
                and 'symbol' in stream:

                endpoint = stream['endpoint']
                symbol = stream['symbol']
                interval = 'none'

                if 'interval' in stream:
                    interval = stream['interval']

                __data = self.__wsm[__exchg].get_current_data(endpoint, symbol, interval)

                error_msg = '\nexchange: ' + str(__exchg) + '\n' +\
                    'endpoint: ' + str(endpoint) + '\n' +\
                    'interval: ' + str(interval) + '\n' +\
                    pprint.pformat(__data, sort_dicts=False)

                __data_struct_res = self.__check_data_structure_local(endpoint, __data)

        if self.__debug:
            self.assertTrue(__data_struct_res, error_msg)
        else:
            self.assertTrue(__data_struct_res)

    def test_bybit(self):
        """
        test_bybit
        ==========
            Test case for bybit exchange.
        """

        __exchg = sys._getframe().f_code.co_name[5:] # pylint: disable=protected-access

        __data_struct_res = False
        error_msg = ''

        for stream in self.__streams:
            if stream is not None\
                and isinstance(stream, dict)\
                and 'endpoint' in stream\
                and 'symbol' in stream:

                endpoint = stream['endpoint']
                symbol = stream['symbol']
                interval = 'none'

                if 'interval' in stream:
                    interval = stream['interval']

                __data = self.__wsm[__exchg].get_current_data(endpoint, symbol, interval)

                error_msg = '\nexchange: ' + str(__exchg) + '\n' +\
                    'endpoint: ' + str(endpoint) + '\n' +\
                    'interval: ' + str(interval) + '\n' +\
                    pprint.pformat(__data, sort_dicts=False)

                __data_struct_res = self.__check_data_structure_local(endpoint, __data)

        if self.__debug:
            self.assertTrue(__data_struct_res, error_msg)
        else:
            self.assertTrue(__data_struct_res)

    def test_kucoin(self):
        """
        test_kucoin
        ===========
            Test case for kucoin exchange.
        """

        __exchg = sys._getframe().f_code.co_name[5:] # pylint: disable=protected-access

        __data_struct_res = False
        error_msg = ''

        for stream in self.__streams:
            if stream is not None\
                and isinstance(stream, dict)\
                and 'endpoint' in stream\
                and 'symbol' in stream:

                endpoint = stream['endpoint']
                symbol = stream['symbol']
                interval = 'none'

                if 'interval' in stream:
                    interval = stream['interval']

                __data = self.__wsm[__exchg].get_current_data(endpoint, symbol, interval)

                error_msg = '\nexchange: ' + str(__exchg) + '\n' +\
                    'endpoint: ' + str(endpoint) + '\n' +\
                    'interval: ' + str(interval) + '\n' +\
                    pprint.pformat(__data, sort_dicts=False)

                __data_struct_res = self.__check_data_structure_local(endpoint, __data)

        if self.__debug:
            self.assertTrue(__data_struct_res, error_msg)
        else:
            self.assertTrue(__data_struct_res)

    def test_okx(self):
        """
        test_okx
        ========
            Test case for okx exchange.
        """

        __exchg = sys._getframe().f_code.co_name[5:] # pylint: disable=protected-access

        __data_struct_res = False
        error_msg = ''

        for stream in self.__streams:
            if stream is not None\
                and isinstance(stream, dict)\
                and 'endpoint' in stream\
                and 'symbol' in stream:

                endpoint = stream['endpoint']
                symbol = stream['symbol']
                interval = 'none'

                if 'interval' in stream:
                    interval = stream['interval']

                __data = self.__wsm[__exchg].get_current_data(endpoint, symbol, interval)

                error_msg = '\nexchange: ' + str(__exchg) + '\n' +\
                    'endpoint: ' + str(endpoint) + '\n' +\
                    'interval: ' + str(interval) + '\n' +\
                    pprint.pformat(__data,sort_dicts=False)

                __data_struct_res = self.__check_data_structure_local(endpoint, __data)

        if self.__debug:
            self.assertTrue(__data_struct_res, error_msg)
        else:
            self.assertTrue(__data_struct_res)

    def test_bingx(self):
        """
        test_bingx
        ==========
            Test case for bingx exchange.
        """

        __exchg = sys._getframe().f_code.co_name[5:] # pylint: disable=protected-access

        __data_struct_res = False
        error_msg = ''

        for stream in self.__streams:
            if stream is not None\
                and isinstance(stream, dict)\
                and 'endpoint' in stream\
                and 'symbol' in stream:

                endpoint = stream['endpoint']
                symbol = stream['symbol']
                interval = 'none'

                if 'interval' in stream:
                    interval = stream['interval']

                __data = self.__wsm[__exchg].get_current_data(endpoint, symbol, interval)

                error_msg = '\nexchange: ' + str(__exchg) + '\n' +\
                    'endpoint: ' + str(endpoint) + '\n' +\
                    'interval: ' + str(interval) + '\n' +\
                    pprint.pformat(__data,sort_dicts=False)

                __data_struct_res = self.__check_data_structure_local(endpoint, __data)

        if self.__debug:
            self.assertTrue(__data_struct_res, error_msg)
        else:
            self.assertTrue(__data_struct_res)

    def test_binanceus(self):
        """
        test_binanceus
        ==============
            Test case for binanceus exchange.
        """

        __exchg = sys._getframe().f_code.co_name[5:] # pylint: disable=protected-access

        __data_struct_res = False
        error_msg = ''

        for stream in self.__streams:
            if stream is not None\
                and isinstance(stream, dict)\
                and 'endpoint' in stream\
                and 'symbol' in stream:

                endpoint = stream['endpoint']
                symbol = stream['symbol']
                interval = 'none'

                if 'interval' in stream:
                    interval = stream['interval']

                __data = self.__wsm[__exchg].get_current_data(endpoint, symbol, interval)

                error_msg = '\nexchange: ' + str(__exchg) + '\n' +\
                    'endpoint: ' + str(endpoint) + '\n' +\
                    'interval: ' + str(interval) + '\n' +\
                    pprint.pformat(__data, sort_dicts=False)

                __data_struct_res = self.__check_data_structure_local(endpoint, __data)

        if self.__debug:
            self.assertTrue(__data_struct_res, error_msg)
        else:
            self.assertTrue(__data_struct_res)


if __name__ == '__main__':

    unittest.main()
