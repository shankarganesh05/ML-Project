# Create an Application instance with Kafka configs


from typing import Dict, List

from loguru import logger
from quixstreams import Application

from src.config import config
from src.Kraken_api.rest import KrakenRestAPIMultipleProducts
from src.Kraken_api.websocket import krakenWebSocketTradeApi
from src.Kraken_api.trade import Trade

def produce_trades(kafka_broker_address: str, kafka_topic_name: str,product_id:List[str],live_or_historical:str,last_n_days:int) -> None:
    app = Application(broker_address=kafka_broker_address)

    topic = app.topic(name=kafka_topic_name, value_serializer='json')

    # event = {"id": "1", "text": "Lorem ipsum dolor sit amet"}
    if live_or_historical == 'live':
        kraken_api = krakenWebSocketTradeApi(product_id=product_id)
    else:
        # to_ms = int(time.time()*1000)
        # from_ms = to_ms - last_n_days * 24 * 60 * 60 * 1000
        kraken_api = KrakenRestAPIMultipleProducts(product_id=product_id,last_n_days=last_n_days)

    logger.info('Creating the producer...')
    with app.get_producer() as producer:
        while True:
        # Create a Producer instance
            
            if kraken_api.is_done():
                logger.info("Done!Fetching Historical Data")
                break
            
            trades: List[Trade] = kraken_api.get_trades()
            for trade in trades:
                # Serialize an event using the defined Topic
                message = topic.serialize(key=trade.product_id, value=trade.model_dump())

                # Produce a message into the Kafka topic
                producer.produce(topic=topic.name, value=message.value, key=message.key)
                logger.info(trade)
        #sleep(1)


if __name__ == '__main__':

    logger.debug(config.model_dump())
    try:
        produce_trades(
            kafka_broker_address=config.kafka_broker_address,
            kafka_topic_name=config.kafka_topic_name,
            product_id = config.product_id,
            live_or_historical= config.live_or_historical,
            last_n_days= config.last_n_days
        )
    except KeyboardInterrupt:
        logger.error("Exiting Gracefully")