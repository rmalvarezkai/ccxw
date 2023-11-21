# pylint: disable=duplicate-code
"""
CCXW - CryptoCurrency eXchange Websocket Library
trades tests cases.

Author: Ricardo Marcelo Alvarez
Date: 2023-10-31
"""
import sys
import os
import unittest
import time
import pprint

from schema import Schema, And, Or

from ccxw import ccxw


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

        cls.__exchanges = ccxw.Ccxw.get_supported_exchanges()
        cls.__endpoint = os.path.basename(__file__)[10:-3]

        __symbol = 'BTC/USDT'
        __testmode = False
        __interval = '1m'
        __result_max_len = 5
        __update_speed = '1000ms'
        __data_max_len = 500
        __debug = False

        for exchange in cls.__exchanges:

            cls.__wsm[exchange] = ccxw.Ccxw(exchange, cls.__endpoint, __symbol, __testmode,\
                                      interval=__interval, result_max_len=__result_max_len,\
                                      update_speed=__update_speed, data_max_len=__data_max_len,\
                                      debug=__debug)

            cls.__wsm[exchange].start()

        time.sleep(9) # For collect some data

    @classmethod
    def tearDownClass(cls):
        for exchange in cls.__exchanges:
            cls.__wsm[exchange].stop()

    def __is_numeric_string(self, value):
        result = False
        try:
            float_value = float(value) # pylint: disable=unused-variable
            result = True
        except ValueError:
            result = False

        return result

    def __check_data_structure(self, data):
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

        if data is not None and schema.is_valid(data):
            if data['data'] is not None and len(data['data']) > 0:
                if schema_data.is_valid(data['data'][0]):
                    if (data['data'][0]['price'] is None or\
                        self.__is_numeric_string(data['data'][0]['price'])) and\
                    (data['data'][0]['quantity'] is None or\
                        self.__is_numeric_string(data['data'][0]['quantity'])):
                        result = True
            else:
                result = True

        return result

    def test_binance(self):
        """
        test_binance
        ============
            Test case for binance exchange.
        """

        __exchg = sys._getframe().f_code.co_name[5:] # pylint: disable=protected-access

        __data = self.__wsm[__exchg].get_current_data()

        error_msg = '\nexchange: ' + str(__exchg) + '\n' +\
            'endpoint: ' + str(self.__endpoint) + '\n' +\
            pprint.pformat(__data,sort_dicts=False)
        __data_struct_res = self.__check_data_structure(__data)

        if self.__debug:
            self.assertTrue(__data_struct_res,error_msg)
        else:
            self.assertTrue(__data_struct_res)

    def test_bybit(self):
        """
        test_bybit
        ==========
            Test case for bybit exchange.
        """

        __exchg = sys._getframe().f_code.co_name[5:] # pylint: disable=protected-access
        __data = self.__wsm[__exchg].get_current_data()

        error_msg = '\nexchange: ' + str(__exchg) + '\n' +\
            'endpoint: ' + str(self.__endpoint) + '\n' +\
            pprint.pformat(__data,sort_dicts=False)
        __data_struct_res = self.__check_data_structure(__data)

        if self.__debug:
            self.assertTrue(__data_struct_res,error_msg)
        else:
            self.assertTrue(__data_struct_res)

    def test_kucoin(self):
        """
        test_kucoin
        ===========
            Test case for kucoin exchange.
        """

        __exchg = sys._getframe().f_code.co_name[5:] # pylint: disable=protected-access
        __data = self.__wsm[__exchg].get_current_data()

        error_msg = '\nexchange: ' + str(__exchg) + '\n' +\
            'endpoint: ' + str(self.__endpoint) + '\n' +\
            pprint.pformat(__data,sort_dicts=False)
        __data_struct_res = self.__check_data_structure(__data)

        if self.__debug:
            self.assertTrue(__data_struct_res,error_msg)
        else:
            self.assertTrue(__data_struct_res)

    def test_okx(self):
        """
        test_okx
        ========
            Test case for okx exchange.
        """

        __exchg = sys._getframe().f_code.co_name[5:] # pylint: disable=protected-access
        __data = self.__wsm[__exchg].get_current_data()

        error_msg = '\nexchange: ' + str(__exchg) + '\n' +\
            'endpoint: ' + str(self.__endpoint) + '\n' +\
            pprint.pformat(__data,sort_dicts=False)
        __data_struct_res = self.__check_data_structure(__data)

        if self.__debug:
            self.assertTrue(__data_struct_res,error_msg)
        else:
            self.assertTrue(__data_struct_res)

    def test_bingx(self):
        """
        test_bingx
        ==========
            Test case for bingx exchange.
        """

        __exchg = sys._getframe().f_code.co_name[5:] # pylint: disable=protected-access
        __data = self.__wsm[__exchg].get_current_data()

        error_msg = '\nexchange: ' + str(__exchg) + '\n' +\
            'endpoint: ' + str(self.__endpoint) + '\n' +\
            pprint.pformat(__data,sort_dicts=False)
        __data_struct_res = self.__check_data_structure(__data)

        if self.__debug:
            self.assertTrue(__data_struct_res,error_msg)
        else:
            self.assertTrue(__data_struct_res)

if __name__ == '__main__':

    unittest.main()
