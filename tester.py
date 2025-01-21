"""
Script to test websocket data
To exec
poetry run python -m unittest tests/test_ccxw.py
"""

import sys
import json
import time
import pprint
import websocket

def __manage_websocket_message(ws, message):
    result = None

    ws_time_diff_limit = 9

    try:
        data_out = json.loads(message)
        pprint.pprint(data_out, sort_dicts=False)
        print('ws_time_diff: ' + str(ws.ws_diff) + ' ===========================================')

        if ws.ws_diff > ws_time_diff_limit:
            ws.close()

        ws.ws_diff = round((time.time() - ws.ws_time_ini), 0)

    except Exception as exc: # pylint: disable=broad-except
        pprint.pprint(exc)

    return result

def __manage_websocket_open(ws):
    result = None

    endpoints = []
    endpoints.append('BTC/FDUSD'.replace("/","").lower() + '@kline_1m')
    endpoints.append('BTC/FDUSD'.replace("/","").lower() + '@trade')
    endpoints.append('BTC/FDUSD'.replace("/","").lower() + '@ticker')
    endpoints.append('BTC/FDUSD'.replace("/","").lower() + '@depth')

    data_open = None
    data_open = {}
    data_open['method'] = 'SUBSCRIBE'
    data_open['params'] = endpoints
    data_open['id'] = int(time.time_ns())

    data_open = json.dumps(data_open)

    print('DATA OPEN: ' + data_open)

    ws.send(data_open)

    return result

# pylint: disable=unused-argument
def __manage_websocket_close(ws, close_status_code,close_msg):
    result = None


    return result


# pylint: disable=unused-argument
def main(argv):
    """
    main function
    =============
    """

    result = 1

    socket = 'wss://stream.binance.com:9443/ws'

    __ws_ping_interval = 30
    __ws_ping_timeout = 10

    ws = websocket.WebSocketApp(socket,\
                                on_message=__manage_websocket_message,\
                                on_open=__manage_websocket_open,\
                                on_close=__manage_websocket_close)

    ws.ws_time_ini = time.time()
    ws.ws_diff = 0

    print('SOCKET: ' + str(socket))

    ws_temp = ws.run_forever(ping_interval=__ws_ping_interval,\
                             ping_timeout=__ws_ping_timeout,\
                             reconnect=320)



    if not ws_temp:
        print('ENDED OK')
    else:
        print('ENDED ERROR')

    result = ws_temp


    return result

if __name__ == "__main__":
    main(sys.argv[1:])
