from typing import List,Dict
from websocket import create_connection
import json

class krakenWebSocketTradeApi:
    url ='wss://ws.kraken.com/v2'

    def __init__(self, product_id:str):

        self.product_id = product_id

        ## Creating Connection using WebSocket
        self._ws=create_connection(self.url)
        print("Connection Established")
        ## Calling Subscribe function for the product_id
        self._subscribe(product_id)
    
    
    ### Subscription Function to Subscribe to Trade Channel for Product_id
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
        self._ws.send(json.dumps(msg)) # converting json to string
        print("Subcribtion Sent")
        """ Discarding initial 2 message due to Subcription Message"""
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
        ## Checking for HearBeat Message and returning Empty List
        if 'heartbeat' in message:
            return []
        
        message = json.loads(message) # converting string to json
        trades = []
        # Iterating over Data in the message and fetching price,qty & timestamp
        for trade in message['data']:
            trades.append({
                'product_id': self.product_id,
                'price': trade['price'],
                'volume': trade['qty'],
                'timestamp': trade['timestamp'],
            })
        return trades


