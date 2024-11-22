#!/usr/bin/python
"""
CCXW - 
Example use.

Author: Ricardo Marcelo Alvarez
Date: 2024-06-20
"""

import os # pylint: disable=unused-import
import sys # pylint: disable=unused-import
import json # pylint: disable=unused-import
import time # pylint: disable=unused-import
import datetime # pylint: disable=unused-import
import random # pylint: disable=unused-import
import getopt # pylint: disable=unused-import
import hmac # pylint: disable=unused-import
import pprint # pylint: disable=unused-import
import threading # pylint: disable=unused-import
import websocket # pylint: disable=unused-import
from ccxw import Ccxw # pylint: disable=unused-import
import ccxw.ccxw_common_functions as ccf # pylint: disable=unused-import

def main(argv): # pylint: disable=unused-argument
    """
    main function
    =============
    """
    result = 0

    __exchanges = ['binance', 'bybit', 'okx', 'bingx', 'kucoin']

    __endpoint = 'order_book'
    __symbol = 'BTC/USDT'

    __result_max_len = 5
    __test_mode = False
    __debug = False

    __streams = [
        {
            'endpoint': __endpoint,
            'symbol': __symbol
        }
    ]

    __total_num_data = 7200
    __diff_limit = 0.5
    __diff_limit_to_prt = __diff_limit
    __first_last_n = 5

    __count_errors = {}

    for __exchange in __exchanges:
        __ccxw_inst = Ccxw(__exchange,\
                            __streams,\
                            result_max_len=__result_max_len,\
                            testmode=__test_mode,\
                            debug=__debug)

        __ccxw_inst.start()

        time.sleep(9)

        __count_errors[__exchange] = 0

        for i in range(0, __total_num_data):
            __res_type = 'both'

            __data_1 = __ccxw_inst.get_current_data(__endpoint, __symbol)
            # pprint.pprint(__data_1, sort_dicts=False)
            # print('=' * 80)

            __bid = round(float(__data_1['data']['bids'][0][0]), 4)
            __ask = round(float(__data_1['data']['asks'][0][0]), 4)
            __diff_1 = __ask - __bid
            __diff_1 = round(__diff_1, 4)

            if abs(__diff_1) > __diff_limit:
                __count_errors[__exchange] += 1

            if abs(__diff_1) > __diff_limit_to_prt or abs(__diff_1) > __diff_limit_to_prt\
                or i <= __first_last_n or i >= (__total_num_data - __first_last_n):
                __to_prt = f'{__exchange}: '
                __to_prt += f' {__bid} - {__ask} = {__diff_1} -> {i}'
                print(__to_prt)
            time.sleep(1)

        __ccxw_inst.stop()
        __to_prt = f'{__exchange}: ERRORS: {__count_errors[__exchange]}'
        print(__to_prt)
        print('=' * 80)
        print('')

    for __exchange, __count_error in __count_errors.items():
        __to_prt = f'{__exchange}: ERRORS: {__count_error}'
        print(__to_prt)

    print('')

    return result

if __name__ == "__main__":
    main(sys.argv[1:])
