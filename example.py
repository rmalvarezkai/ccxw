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
#import json

import pprint # For debug

from ccxw import Ccxw
#from ccxw.binance import BinanceCcxwAuxClass
#from ccxw.dict_safe_thread import DictSafeThread
#import ccxw.ccxw_common_functions as ccf

def main(argv): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
    """
    main function
    =============
    """

    result = 1

    # data = DictSafeThread()
    # othr = DictSafeThread()

    # data['key'] = 'value'
    # data[5] = 'Son 5'

    # othr[5] = 'Son 5'
    # othr['key'] = 'value'

    # print(data)

    # print(str(data))

    # print(str(data['key']))
    # print(str(data[5]))

    # print('LEN: ' + str(len(data)))

    # sort_dicts = True

    # pprint.pprint(data, sort_dicts=sort_dicts)
    # pprint.pprint(othr, sort_dicts=sort_dicts)

    # print('EQ: ' + str(data == othr))
    # print('HASH data: ' + str(hash(data)))
    # print('HASH othr: ' + str(hash(othr)))

    # return result



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


    # # Esto es para ver el dia que se comenzo a listar el simbolo
    # symbol = symbol_in.replace('/', '')

    # get_url = (
    #     f'https://api.binance.com/api/v1/klines?symbol={symbol}&interval={interval}&startTime=0'
    # )

    # data = ccf.file_get_contents_url(get_url)

    # if ccf.is_json(data):
    #     data = json.loads(data)
    # else:
    #     data = None

    # if data is not None and isinstance(data, list) and len(data) > 0:

    #     date_time = round(int(data[0][0]) / 1000)
    #     date_date = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(date_time)))

    #     print(f'TIME: {date_time}')
    #     print(f'DATE: {date_date}')

    # return result


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

    streams = [
                    {
                        'endpoint': 'order_book',
                        'symbol': symbol_in
                    },
                    {
                        'endpoint': 'kline',
                        'symbol': symbol_in,
                        'interval': interval
                    },
                    {
                        'endpoint': 'trades',
                        'symbol': symbol_in
                    },
                    {
                        'endpoint': 'ticker',
                        'symbol': symbol_in
                    }
            ]

    # streams = None
    # if endpoint == 'kline':
    #     streams = [
    #                     {
    #                         'endpoint': str(endpoint),
    #                         'symbol': symbol_in,
    #                         'interval': interval
    #                     }
    #             ]
    # else:
    #     streams = [
    #                     {
    #                         'endpoint': str(endpoint),
    #                         'symbol': symbol_in
    #                     }
    #             ]

    # streams = [
    #                 {
    #                     'endpoint': 'kline',
    #                     'symbol': symbol_in,
    #                     'interval': interval
    #                 }
    #         ]

    # streams = [
    #                 {
    #                     'endpoint': 'trades',
    #                     'symbol': symbol_in
    #                 }
    #         ]

    wsm0 = None

    time_max = 50
    time_sleep = 5
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
    # aux_class = BinanceCcxwAuxClass(streams, debug=True)
    # return result
    #############################

    #############################
    if proc_only_arguments: # pylint: disable=too-many-nested-blocks
        try:
            pprint.pprint(streams, sort_dicts=False)
            wsm0 = Ccxw(exchange_out, streams, testmode=testmode,\
                            result_max_len=result_max_len,\
                            data_max_len=data_max_len, debug=debug)

        except Exception as exc: # pylint: disable=broad-except
            print('EXCEPTION: ' + str(exc))
            wsm0 = None

        # return result

        if wsm0 is not None:
            # print('ACA: 0')
            # wsm0.start()
            # print('ACA: 1')
            # time.sleep(9)
            # print('ACA: 2')
            # wsm0.stop()
            # print('ACA: 3')
            # time.sleep(2.5)
            # print('ACA: 4')
            # time.sleep(2.5)

            wsm0.start()
            time.sleep(50)

            for i in range(0,int(round(time_max / time_sleep))):
                for stream in streams:
                    __interval = 'none'
                    if 'interval' in stream:
                        __interval = stream['interval']

                    # print('D: ' + str(stream['endpoint'])\
                    #       + ', ' + str(stream['symbol'])\
                    #       + ', ' + str(__interval))
                    data0 = wsm0.get_current_data(stream['endpoint'], stream['symbol'], __interval)
                    # data0 = None

                    if data0 is not None:
                        pprint.pprint(data0, sort_dicts=False)
                        print('')
                        print(str(i) + '    ' + '=' * 80)
                    # else:
                    #     print('NO DATA')
                    #     print(str(i) + '    ' + '=' * 80)

                    time.sleep(1)
                    # __db_size = wsm0.get_sqlite_memory_used()
                    # print('++++++++++++++++++++++++++++++++++++++++++++++++++')
                    # print(f'Sqlite database size: {__db_size}')
                    # __db_size = wsm0.get_sqlite_memory_used_human_readable()
                    # print(f'Sqlite database size: {__db_size}')
                    # print('++++++++++++++++++++++++++++++++++++++++++++++++++')

                time.sleep(time_sleep)

            time.sleep(5)
            wsm0.stop()
            # print('ACA: 5')
            # print('=========================================================================')

    else:
        min_get_time = None
        max_get_time = None

        for endpoint_out in Ccxw.get_supported_endpoints():
            for exchange_out in Ccxw.get_supported_exchanges():
                try:
                    wsm0 = Ccxw(exchange_out, streams, testmode,\
                                    result_max_len=result_max_len,\
                                    data_max_len=data_max_len,debug=debug)
                except Exception as exc: # pylint: disable=broad-except
                    print(str(exc))
                    wsm0 = None

                if wsm0 is not None:
                    wsm0.start()
                    time.sleep(5)

                    for i in range(0,int(round(time_max / time_sleep))):
                        for stream in streams:
                            __interval = 'none'
                            if 'interval' in stream:
                                __interval = stream['interval']

                            time_ini = time.time_ns()
                            data0 = wsm0.get_current_data(stream['endpoint'],\
                                                          stream['symbol'],\
                                                          __interval)
                            time_end = time.time_ns()
                            time_diff = time_end - time_ini

                            if min_get_time is None or time_diff < min_get_time:
                                min_get_time = time_diff

                            if max_get_time is None or time_diff > max_get_time:
                                max_get_time = time_diff

                        ##data0 = None

                            # if data0 is not None:
                            #     pprint.pprint(data0, sort_dicts=False)
                            #     print('')
                            #     print(str(i) + '    ' +\
                            #         '-----------------------------------------------------------')

                            time.sleep(1)

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
