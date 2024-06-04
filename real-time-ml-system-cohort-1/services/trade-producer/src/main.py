# Create an Application instance with Kafka configs
from typing import List,Dict
from quixstreams import Application
from time import sleep
from src.kraken_api import krakenWebSocketTradeApi

def produce_trades(kafka_broker_address:str,kafka_topic_name:str)-> None:

    app = Application(broker_address=kafka_broker_address)

    topic = app.topic(name=kafka_topic_name, value_serializer='json')

    #event = {"id": "1", "text": "Lorem ipsum dolor sit amet"}
    kraken_api = krakenWebSocketTradeApi(product_id='BTC/USD')
    

    while True:
        # Create a Producer instance
        with app.get_producer() as producer:

            trades : List[Dict] = kraken_api.get_trades()
            for trade in trades:

                # Serialize an event using the defined Topic 
                message = topic.serialize(key=trade["product_id"], value=trade)

                # Produce a message into the Kafka topic
                producer.produce(
                topic=topic.name, value=message.value, key=message.key
                )
                print('Message Sent')
        sleep(1)
if __name__ == '__main__':
    produce_trades(kafka_broker_address='localhost:19092',kafka_topic_name='trade')

