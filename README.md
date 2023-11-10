
## CCXW - CryptoCurrency eXchange Websocket Library

This library is designed to fetch data from exchange WebSocket APIs and store or update data in a temporary SQLite database in the background, making it readily accessible for retrieval. 

Currently, it is available for several exchanges, including Binance, Bybit, and more. You can find a complete list of supported exchanges using `Ccxw.get_supported_exchanges()`.

Furthermore, the library supports a variety of endpoints, such as order_book, kline, and more. A comprehensive list of supported endpoints can be found via `Ccxw.get_supported_endpoints()`.

### Example:

```python
import time
import pprint
from ccxw import Ccxw

exchange = 'binance'
endpoint = 'order_book'
symbol = 'BTC/USDT'

wsm = ccxw.Ccxw(exchange, endpoint, symbol, result_max_len=20)  # Create instance

wsm.start()  # Start getting data

time.sleep(2)  # Wait for available data

for i in range(0, 10):
    data = wsm.get_current_data()
    pprint.pprint(data, sort_dicts=False)
    time.sleep(1)

wsm.stop()  # Stop getting data
```

### Important Information

Please be aware that each instance opens a new connection to websockets. If you create multiple instances for the same exchange, you may exceed the websockets connection limits set by exchanges. Make sure to check the connection limits of exchanges before opening numerous instances.

[View License](LICENSE)




