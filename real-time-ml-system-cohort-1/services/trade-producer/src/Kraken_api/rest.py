from typing import List, Dict
import json
import requests

from loguru import logger

class KrakenRestAPI:
    URL = "https://api.kraken.com/0/public/Trades?pair={product_id}&since={since_sec}"

    def __init__(
            self,
            product_id:List[str],
            from_ms:int,
            to_ms:int,
    )-> None:
        self.product_id = product_id
        self.from_ms = from_ms
        self.to_ms = to_ms
    
    def get_trades(self)-> List[Dict]:


        payload = {}
        headers = {'Accept': 'application/json'}

        since_sec = self.from_ms // 1000
        url= self.URL.format(product_id = self.product_id[0],since_sec = since_sec)
        response = requests.request("GET", url, headers=headers, data=payload)

        # parse string into dictionary
        data = json.loads(response.text)

        trades= []
        for trade in data['result'][self.product_id[0]]:
            trades.append({
                'price': float(trade[0]),
                'volume': float(trade[1]),
                'time' : int(trade[2]),
                'product_id': self.product_id[0]
                
            })
        # check if we are done fetching historical data
        last_ts_in_ns = int(data['result']['last'])
        # convert nanoseconds to milliseconds
        last_ts = last_ts_in_ns // 1_000_000
        if last_ts >= self.to_ms:
            # yes, we are done
            self._is_done = True

        logger.debug(f'Fetched {len(trades)} trades')
        # log the last trade timestamp
        logger.debug(f'Last trade timestamp: {last_ts}')

        return trades
    def is_done(self) -> bool:
        """The websocket never stops, so we never stop fetching trades."""
        return self._is_done

    
