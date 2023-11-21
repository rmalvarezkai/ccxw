#!/usr/bin/python
"""
CCXW - CryptoCurrency eXchange Websocket Library
Example use.

Author: Ricardo Marcelo Alvarez
Date: 2023-10-31
"""

import os.path
import getopt
import sys
import time

import pprint # For debug

from ccxw import ccxw

def main(argv): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
    """
    main function
    =============
    """

    result = 1

    exchange = "NOT"
    symbol_in = "NOT"
    interval = "NOT"
    reference_asset = 'NOT'
    endpoint = 'order_book'

    help_msg = os.path.basename(__file__)\
        + ' -e <exchange> -s <symbol> -i <interval> [ -r <reference_asset> ] [-p <endpoint> ]'

    try:
        optlist, args = (  # pylint: disable=unused-variable
            getopt.getopt(argv,'he:s:i:r:p:',\
                          ["exchange=","symbol=","interval=","reference_asset=","endpoint="])
        )
    except getopt.GetoptError:
        print(help_msg)
        sys.exit(2)

    for opts, arg in optlist:
        if opts == '-h':
            print(help_msg)
            sys.exit()
        elif opts in ("-e", "--exchange"):
            exchange = arg
        elif opts in ("-s", "--symbol"):
            symbol_in = arg
        elif opts in ("-i", "--interval"):
            interval = arg
        elif opts in ("-r", "--reference_asset"):
            reference_asset = arg
        elif opts in ("-p", "--endpoint"):
            endpoint = arg

    if len(exchange) == 0 or len(symbol_in) == 0 or len(interval) == 0:
        print(help_msg)
        sys.exit(2)

    msg_out = "EXCHANGE: " + exchange\
        +  ", SYMBOL: " + symbol_in\
        + ", INTERVAL: " + interval\
        + ", REFERENCE_ASSET: " + reference_asset\
        + ", ENDPOINT: " + str(endpoint)
    print(msg_out)
    print('')

    ################################################################################################
    # api_key = None
    # api_secret = None

    # testmode = False
    # result_max_len = 4
    # update_speed = '1000ms'
    # data_max_len = 4
    # debug = False
    # wsm = None

    # #max_symbols_lists = 0

    # for exchange_out in ccxw.Ccxw.get_supported_exchanges():
    #     try:
    #         wsm = ccxw.Ccxw(exchange_out, endpoint, symbol_in, testmode,\
    #                         api_key=api_key, api_secret=api_secret, interval=interval,\
    #                         result_max_len=result_max_len, update_speed=update_speed,\
    #                         data_max_len=data_max_len, debug=debug)
    #     except Exception as exc: # pylint: disable=broad-except
    #         print(str(exc))
    #         wsm = None

    #     time_ini = int(time.time_ns())
    #     data = wsm.get_exchange_info(False) # # pylint: disable=unused-variable
    #     time_end = int(time.time_ns())
    #     time_diff = round(((time_end - time_ini) / 1000000),3)
    #     print(exchange_out + ' -> ' + 'time_diff: ' + str(time_diff))
    #     time_ini = int(time.time_ns())
    #     data = wsm.get_exchange_info(False) # # pylint: disable=unused-variable
    #     time_end = int(time.time_ns())
    #     time_diff = round(((time_end - time_ini) / 1000000),3)
    #     print(exchange_out + ' -> ' + 'time_diff: ' + str(time_diff))

    #     print('-----------------------------------------------------------')

    #     #pprint.pprint(data,sort_dicts=False)

    # #     data = wsm.get_exchange_full_list_symbols(True)

    # #     exchange_symbols_len = len(data)

    # #     print(str(exchange_out) + ': ' + str(exchange_symbols_len))

    # #     if exchange_symbols_len > max_symbols_lists:
    # #         max_symbols_lists = exchange_symbols_len

    # # print('max_symbols_lists: ' + str(max_symbols_lists))

    # return result
    ################################################################################################

    ##pprint.pprint(getattr(ccxt.pro,exchange)().describe()['urls'])
    ##return result

    ##endpoint = 'order_book'
    ##endpoint = 'kline'
    ##endpoint = 'trades'

    wsm0 = None

    api_key = None
    api_secret = None

    time_max = 5
    time_sleep = 1
    testmode = False
    result_max_len = 4
    #result_max_len = 500
    update_speed = '1000ms'
    data_max_len = 4
    data_max_len = 500
    debug = False

    exchange_out = exchange
    endpoint_out = endpoint

    proc_only_arguments = True

    #############################
    if proc_only_arguments: # pylint: disable=too-many-nested-blocks
        try:
            wsm0 = ccxw.Ccxw(exchange_out, endpoint_out, symbol_in, testmode,\
                            api_key=api_key, api_secret=api_secret, interval=interval,\
                            result_max_len=result_max_len, update_speed=update_speed,\
                            data_max_len=data_max_len, debug=debug)
        except Exception as exc: # pylint: disable=broad-except
            print(str(exc))
            wsm0 = None

        if wsm0 is not None:
            wsm0.start()
            time.sleep(5)

            for i in range(0,int(round(time_max / time_sleep))):

                data0 = wsm0.get_current_data()
                ##data0 = None

                if data0 is not None:
                    pprint.pprint(data0, sort_dicts=False)
                    print('')
                    print(str(i) + '    ' +\
                        '========================================================================')

                time.sleep(time_sleep)

            time.sleep(5)
            wsm0.stop()

    else:
        min_get_time = None
        max_get_time = None

        for endpoint_out in ccxw.Ccxw.get_supported_endpoints():
            for exchange_out in ccxw.Ccxw.get_supported_exchanges():
                try:
                    wsm0 = ccxw.Ccxw(exchange_out, endpoint_out, symbol_in, testmode,\
                                    api_key=api_key, api_secret=api_secret, interval=interval,\
                                    result_max_len=result_max_len, update_speed=update_speed,\
                                    data_max_len=data_max_len,debug=debug)
                except Exception as exc: # pylint: disable=broad-except
                    print(str(exc))
                    wsm0 = None

                if wsm0 is not None:
                    wsm0.start()
                    time.sleep(5)

                    for i in range(0,int(round(time_max / time_sleep))):

                        time_ini = time.time_ns()
                        data0 = wsm0.get_current_data()
                        time_end = time.time_ns()
                        time_diff = time_end - time_ini

                        if min_get_time is None or time_diff < min_get_time:
                            min_get_time = time_diff

                        if max_get_time is None or time_diff > max_get_time:
                            max_get_time = time_diff

                        ##data0 = None

                        if data0 is not None:
                            pprint.pprint(data0, sort_dicts=False)
                            print('')
                            print(str(i) + '    ' +\
                                '----------------------------------------------------------------')

                        time.sleep(time_sleep)

                    wsm0.stop()
                print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
                wsm0 = None
                time.sleep(5)
            print('===============================================================================')
            print('')

        min_get_time = round((min_get_time / 1000000),3)
        max_get_time = round((max_get_time / 1000000),3)

        print('min_get_time (ms): ' + str(min_get_time))
        print('max_get_time (ms): ' + str(max_get_time))

    return result

if __name__ == "__main__":
    main(sys.argv[1:])
