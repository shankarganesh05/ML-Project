# Create an Application instance with Kafka configs
import time
from time import sleep
from typing import Dict, List

from loguru import logger
from quixstreams import Application

from src import config
from src.Kraken_api.websocket import krakenWebSocketTradeApi
from src.Kraken_api.rest import KrakenRestAPI


def produce_trades(kafka_broker_address: str, kafka_topic_name: str,live_or_historical:str,last_n_days:int) -> None:
    app = Application(broker_address=kafka_broker_address)

    topic = app.topic(name=kafka_topic_name, value_serializer='json')

    # event = {"id": "1", "text": "Lorem ipsum dolor sit amet"}
    if live_or_historical == 'live':
        kraken_api = krakenWebSocketTradeApi(product_id=config.product_id)
    else:
        to_ms = int(time.time()*1000)
        from_ms = to_ms - last_n_days * 24 * 60 * 60 * 1000
        kraken_api = KrakenRestAPI(product_id=config.product_id,from_ms=from_ms,to_ms=to_ms)

    logger.info('Creating the producer...')
    with app.get_producer() as producer:
        while True:
        # Create a Producer instance
            
            if kraken_api.is_done  == True:
                logger.info("Fetching Done")
                break
            
            trades: List[Dict] = kraken_api.get_trades()
            for trade in trades:
                # Serialize an event using the defined Topic
                message = topic.serialize(key=trade['product_id'], value=trade)

                # Produce a message into the Kafka topic
                producer.produce(topic=topic.name, value=message.value, key=message.key)
                logger.info(trade)
        #sleep(1)


if __name__ == '__main__':

    try:
        produce_trades(
            kafka_broker_address=config.kafka_broker_address,
            kafka_topic_name=config.kafka_topic_name,
            live_or_historical= config.live_or_historical,
            last_n_days= config.last_n_days
        )
    except KeyboardInterrupt:
        logger.error("Exiting Gracefully")