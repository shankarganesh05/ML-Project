from typing import List,Dict
from websocket import create_connection
import json

class krakenWebSocketTradeApi:
    url ='wss://ws.kraken.com/v2'

    def __init__(self, product_id:str):

        self.product_id = product_id

        self._ws=create_connection(self.url)
        print("Connection Established")
        self._subscribe(product_id)
    
    
    def _subscribe(self,product_id):
        msg = { 
            "method": "subscribe",
            "params": {
                "channel": "trade",
                "symbol": [
                    product_id
                ],
                "snapshot": False
            }
        }
        self._ws.send(json.dumps(msg))
        print("Subcribtion Sent")
        """ Discarding initial 2 message due to Subcription"""
        _= self._ws.recv()
        _= self._ws.recv()



    def get_trades(self)-> List[Dict]:

        # mock_trades=[
        #     {
        #     'product-id':'BTC/USD',
        #     'price':60000,
        #     'volume': 0.01,
        #     'timestamp': 1717469070,
        # },
        #  {
        #     'product-id':'BTC/USD',
        #     'price':61000,
        #     'volume': 0.01,
        #     'timestamp': 1717479070,
        # }
        # ]
        message = self._ws.recv()
        if 'heartbeat' in message:
            return []
        
        message = json.loads(message)
        trades = []
        for trade in message['data']:
            trades.append({
                'product_id': self.product_id,
                'price': trade['price'],
                'volume': trade['qty'],
                'timestamp': trade['timestamp'],
            })
        return trades


